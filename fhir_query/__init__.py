# Initialize the fhir_query package
import asyncio
import concurrent
import json
import logging
import sqlite3
from urllib.parse import urlparse

from nested_lookup import nested_lookup
import tempfile
from collections import defaultdict
from typing import Any, Optional, Callable

import httpx
from dotty_dict import dotty
from halo import Halo

UNKNOWN_CATEGORY = {"coding": [{"system": "http://snomed.info/sct", "code": "261665006", "display": "Unknown"}]}


def setup_logging(debug: bool, log_file: str) -> None:
    """ """
    log_level = logging.DEBUG if debug else logging.INFO
    file_handler = logging.FileHandler(log_file)
    logging.basicConfig(level=log_level, handlers=[file_handler])

    # Configure httpx logger
    httpx_logger = logging.getLogger("httpx")
    httpx_logger.setLevel(log_level)
    httpx_logger.addHandler(file_handler)


class ResourceDB:
    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize the ResourceDB instance and create the resources table if it doesn't exist.
        :param db_path: Path to the SQLite database file (default is in-memory database).
        """
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        self._logged_already: list[str] = []
        self.adds_counters: dict[str, int] = defaultdict(int)
        self._initialize_table()

    def _initialize_table(self) -> None:
        """
        Create the 'resources' table if it doesn't already exist.
        """
        with self.connection:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS resources (
                    id VARCHAR NOT NULL,
                    resource_type VARCHAR NOT NULL,
                    key VARCHAR NOT NULL,
                    resource JSON NOT NULL,
                    PRIMARY KEY (id, resource_type)
                )
                """
            )
            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_resources_key
                ON resources (key)
                """
            )

    def add(self, resource: dict[str, Any]) -> None:
        """
        Add a resource to the 'resources' table.
        Add a resource to the 'resources' table.
        :param resource: A dictionary with 'id', 'resourceType', and other fields.
        """
        if "id" not in resource or "resourceType" not in resource:
            raise ValueError("Resource must contain 'id' and 'resourceType' fields.")

        try:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO resources (id, resource_type, key, resource)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        resource["id"],
                        resource["resourceType"],
                        f'{resource["resourceType"]}/{resource["id"]}',
                        json.dumps(resource),
                    ),
                )
                self.adds_counters[resource["resourceType"]] += 1
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                pass
            else:
                raise

    def all_keys(self, resource_type: str) -> list[Any]:
        """
        Retrieve all (id, resource_type) tuples for a given resource_type.
        :param resource_type: The resource type to filter by.
        :return: A list of tuples (id, resource_type).
        """
        with self.connection:
            cursor = self.connection.execute(
                """
                SELECT id, resource_type
                FROM resources
                WHERE resource_type = ?
            """,
                (resource_type,),
            )
            return cursor.fetchall()

    def all_resources(self, resource_type: str) -> list[dict[str, Any]]:
        """
        Retrieve all resource dicts for a given resource_type.
        :param resource_type: The resource type to filter by.
        :return: A list of dicts.
        """
        with self.connection:
            cursor = self.connection.execute(
                """
                SELECT resource
                FROM resources
                WHERE resource_type = ?
            """,
                (resource_type,),
            )
            return [json.loads(row[0]) for row in cursor.fetchall()]

    def count_resource_types(self) -> dict[str, Any]:
        """
        Count the number of resources for each resource_type.
        :return: A dictionary with resource_type as keys and counts as values.
        """
        with self.connection:
            cursor = self.connection.execute(
                """
                   SELECT resource_type, COUNT(*)
                   FROM resources
                   GROUP BY resource_type
               """
            )
            return {row[0]: row[1] for row in cursor.fetchall()}

    def close(self) -> None:
        """
        Close the database connection.
        """
        self.connection.close()

    def aggregate(self) -> dict:
        """Aggregate metadata counts resourceType(count)-count->resourceType(count)."""

        nested_dict: Callable[[], defaultdict[str, defaultdict]] = lambda: defaultdict(defaultdict)

        count_resource_types = self.count_resource_types()

        summary = nested_dict()

        for resource_type in count_resource_types:
            resources = self.all_resources(resource_type)
            for _ in resources:

                if "count" not in summary[resource_type]:
                    summary[resource_type]["count"] = 0
                summary[resource_type]["count"] += 1

                refs = nested_lookup("reference", _)
                for ref in refs:
                    # A codeable reference is an object with a codeable concept and a reference
                    if isinstance(ref, dict):
                        ref = ref["reference"]
                    ref_resource_type = ref.split("/")[0]
                    if "references" not in summary[resource_type]:
                        summary[resource_type]["references"] = nested_dict()
                    dst = summary[resource_type]["references"][ref_resource_type]
                    if "count" not in dst:
                        dst["count"] = 0
                    dst["count"] += 1

        return summary


class GraphDefinitionRunner(ResourceDB):
    """
    A class to parse a FHIR GraphDefinition and execute the queries defined in its links.

    See
    https://www.devdays.com/wp-content/uploads/2021/12/Rene-Spronk-GraphDefinition-_-DevDays-2019-Amsterdam-1.pdf
    """

    def __init__(self, fhir_base_url: str, db_path: Optional[str] = None, debug: Optional[bool] = False):
        """
        Initializes the GraphDefinitionRunner.

        Args:
            fhir_base_url (str): Base URL of the FHIR server.
        """
        if not db_path:
            # initializes the ResourceDB to a temporary file
            db_path = tempfile.NamedTemporaryFile(delete=False).name

        super().__init__(db_path)
        self.fhir_base_url = fhir_base_url
        self.max_requests = 10
        self.debug = debug

    async def fetch_graph_definition(self, graph_definition_id: str) -> Any:
        """
        Fetches the GraphDefinition resource from the FHIR server.

        Args:
            graph_definition_id (str): ID of the GraphDefinition resource.

        Returns:
            dict: Parsed JSON response of the GraphDefinition resource.
        """
        async with httpx.AsyncClient() as client:
            url = f"{self.fhir_base_url}/GraphDefinition/{graph_definition_id}"
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

    async def execute_query(self, query_url: str) -> list[dict[str, Any]]:
        """
        Executes a FHIR query for a given URL, returns all pages as a list of resources.

        Args:
            query_url (str): Fully constructed query URL.

        Yields:
            dict: A resource from the query result.
        """
        retry = 0
        max_retry = 3

        while retry < max_retry:
            async with httpx.AsyncClient() as client:
                try:
                    if self.debug:
                        logging.info(f"Querying: {query_url}")
                    response = await client.get(query_url, timeout=300)
                    response.raise_for_status()
                    query_result = response.json()
                    resources = []
                    next_link = [link for link in query_result.get("link", []) if link["relation"] == "next"]
                    for entry in query_result.get("entry", []):
                        self.add(entry["resource"])
                        resources.append(entry["resource"])
                    if next_link:
                        for entry in await self.execute_query(next_link[0]["url"]):
                            resources.append(entry)

                    return resources

                except httpx.ReadTimeout as e:
                    if retry == max_retry:
                        logging.warning(f"ReadTimeout: {e} sleeping for 5 seconds. Retry: {retry}")
                    await asyncio.sleep(5)
                    retry += 1
                except httpx.ConnectTimeout as e:
                    if retry == max_retry:
                        logging.warning(f"ConnectTimeout: {e} sleeping for 5 seconds. Retry: {retry}")
                    await asyncio.sleep(5)
                    retry += 1

        return []

    async def process_links(
        self,
        parent_target_id: str,
        parent_resources: list[dict[str, Any]],
        graph_definition: dict[str, Any],
        visited: set[tuple[Any, Any, Any]],
        spinner: Halo,
    ) -> None:
        """
        Processes all links in the GraphDefinition for the given resource.

        Args:
            parent_target_id (str): The resource_type of the parent resource.
            parent_resources (generator dict): resources returned from the last query, can have multiple resource types.
            graph_definition (dict): The entire GraphDefinition resource.
            visited (set): Set of visited node-resource combinations to prevent cycles.
            spinner (Halo): Spinner object to show progress

        Returns:
            dict: Aggregated results from all traversed links.
        """

        links = [link for link in graph_definition.get("link", []) if link.get("sourceId") == parent_target_id]

        for link in links:
            path = link.get("path", None)
            params = link.get("params", None)
            target_id = link["targetId"]
            source_id = link["sourceId"]
            if params:

                # create a parent resource and extract with the path
                current_path = set()
                for _ in parent_resources:
                    if _["resourceType"] == source_id:
                        key = (_["resourceType"], _["id"], target_id)
                        if key not in visited:
                            visited.add(key)
                            # since path can point to anywhere in the resource, we need the full resource
                            parent = dotty({_["resourceType"]: _})
                            assert path, f"Path is required for {link}"

                            #  assert path in parent, f"Path {path} not found in {parent}"
                            if path not in parent:
                                continue

                            _path = parent[path]
                            if path.endswith(".id"):
                                _path = source_id + "/" + _path
                            current_path.add(_path)
                # if not current_path:
                #     continue
                #     # assert (
                #     #     current_path
                #     # ), f"Could not find any resources for {source_id} link: {link}"

                if spinner:
                    spinner.succeed()
                    spinner.start(
                        text=f"Processing link: {link['targetId']}/{link['params']} with {len(current_path)} {link['sourceId']}(s)"
                    )

                # handle current path <= chunk size
                _current_path: list[Any] = list(current_path)
                chunk_size = 50
                chunks = [_current_path]
                tasks = []
                if len(_current_path) > chunk_size:
                    chunks = [_current_path[i : i + chunk_size] for i in range(0, len(_current_path), chunk_size)]
                for chunk in chunks:
                    _params = params.replace("{path}", ",".join(chunk))
                    query_url = f"{self.fhir_base_url}/{target_id}?{_params}"
                    tasks.append(asyncio.create_task(self.execute_query(query_url)))
                    if len(tasks) >= self.max_requests:
                        await asyncio.gather(*tasks)
                        tasks = []
                await asyncio.gather(*tasks)
            else:
                logging.debug(f"No `params` property found in link. {link} continuing")

            # get all resources for target
            query_results = self.all_resources(target_id)

            # show intermediate results
            logging.debug(self.count_resource_types())

            # are there any other links from this target_id to follow?

            edges = [edge for edge in graph_definition.get("link", []) if edge.get("sourceId") == target_id]
            if edges:
                await self.process_links(
                    parent_target_id=target_id,
                    parent_resources=query_results,
                    graph_definition=graph_definition,
                    visited=visited,
                    spinner=spinner,
                )
            else:
                pass

        # if spinner:
        #     spinner.succeed()
        # f"Processed link: {link['targetId']}/{link['params']} with {len(current_path)} {link['sourceId']}(s)")

    async def run(
        self,
        graph_definition: dict[str, Any],
        path: str,
        spinner: Halo,
    ) -> None:
        """
        Runs the GraphDefinition queries starting from the specified resource.

        Args:
            graph_definition (dict): The GraphDefinition resource.
            path (str): Path to query the FHIR server and pass to the GraphDefinition.
            spinner (Halo): Spinner object to show progress.

        Returns:
            dict: Aggregated results of all traversed resources.
        """
        visited: set[tuple[Any, Any, Any]] = set()

        if path:
            url = self.fhir_base_url + path
            parent_resources = await self.execute_query(url)
        else:
            parent_resources = []

        start_resource_type = graph_definition["link"][0]["sourceId"]

        return await self.process_links(
            parent_resources=parent_resources,
            parent_target_id=start_resource_type,
            graph_definition=graph_definition,
            visited=visited,
            spinner=spinner,
        )


def tree() -> defaultdict:
    """A recursive defaultdict."""
    return defaultdict(tree)


class VocabularyRunner:
    def __init__(self, fhir_base_url: str):
        """
        Initialize the VocabularyRunner instance.
        :param fhir_base_url: Base URL of the FHIR server.
        """
        self.fhir_base_url = fhir_base_url

    async def fetch_resource(self, resource_type: str, spinner: Halo = None) -> dict[str, dict[Any, Any]]:
        """
        Fetch resources of a given type from the FHIR server.
        :param spinner: A Halo spinner object to show progress.
        :param resource_type: The type of resource to fetch.
        :return: A list of resources.
        """
        counts: dict = {resource_type: {}}
        category_counts = counts[resource_type]
        # A client with a 60s timeout for connecting, and a 10s timeout elsewhere.
        timeout = httpx.Timeout(10.0, connect=60.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            page_count = 1
            url = f"{self.fhir_base_url}/{resource_type}?_count=1000&_total=accurate&_elements=category,code,type"
            while url:
                if spinner:
                    spinner.text = f"Fetching {resource_type} page {page_count}"
                response = await client.get(url)
                response.raise_for_status()
                page_count += 1
                data = response.json()
                for entry in data.get("entry", []):
                    resource = entry["resource"]
                    # get the code, if not there, get the type
                    code = resource.get("code", resource.get("type", None))
                    if not code:
                        code = UNKNOWN_CATEGORY
                    for category in resource.get("category", [UNKNOWN_CATEGORY]):
                        for category_coding in category.get("coding", []):
                            assert "display" in category_coding, f"No 'display' property in coding: {category_coding}"
                            if category_coding["display"] not in category_counts:
                                category_counts[category_coding["display"]] = {}

                            code_counts = category_counts[category_coding["display"]]
                            for code_coding in code.get("coding", []):
                                assert "display" in code_coding, f"No 'display' property in coding: {code_coding}"
                                if code_coding["display"] not in code_counts:
                                    code_counts[code_coding["display"]] = 0
                                code_counts[code_coding["display"]] += 1
                next_link = next((link["url"] for link in data.get("link", []) if link["relation"] == "next"), None)
                if next_link:
                    assert "write-fhir" not in next_link, f"Found write-fhir in from {url} next link: {next_link}"
                url = next_link
        return counts

    async def collect(self, resource_types: list[str], spinner: Halo = None) -> list:
        """
        Collect vocabularies from the specified resource types.
        :param spinner: A Halo spinner object to show progress.
        :param resource_types: A list of resource types to collect vocabularies from.
        """
        tasks = []

        for resource_type in resource_types:
            tasks.append(asyncio.create_task(self.fetch_resource(resource_type, spinner)))

        results = await asyncio.gather(*tasks)
        return results
