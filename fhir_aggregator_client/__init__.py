# Initialize the fhir_query package
import asyncio
import json
import logging
import sqlite3
import sys
import tempfile
from collections import defaultdict
from typing import Any, Optional, Callable

import httpx
from dotty_dict import dotty
from halo import Halo
import os

UNKNOWN_CATEGORY = {"coding": [{"system": "http://snomed.info/sct", "code": "261665006", "display": "Unknown"}]}


def ensure_our_directory() -> str:
    # Get the home directory path
    home_dir = os.path.expanduser("~")
    # Define the new directory path
    our_directory = os.path.join(home_dir, ".fhir-aggregator")
    if not os.path.exists(our_directory):
        print(f"Creating directory: {our_directory}", file=sys.stderr)
    # Create the directory
    os.makedirs(our_directory, exist_ok=True)
    return our_directory


def setup_logging(debug: bool, log_file: str) -> None:
    """
    Set up logging configuration.

    Args:
        debug (bool): Enable debug mode if True.
        log_file (str): Path to the log file.
    """
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

        Args:
            db_path (str): Path to the SQLite database file (default is in-memory database).
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

        Args:
            resource (dict): A dictionary with 'id', 'resourceType', and other fields.
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

        Args:
            resource_type (str): The resource type to filter by.

        Returns:
            list: A list of tuples (id, resource_type).
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

        Args:
            resource_type (str): The resource type to filter by.

        Returns:
            list: A list of dicts.
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

        Returns:
            dict: A dictionary with resource_type as keys and counts as values.
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

    def aggregate(self, ignored_edges=None) -> dict:
        """
        Aggregate metadata counts resourceType(count)-count->resourceType(count).

        Args:
            ignored_edges (list): List of edges to ignore during aggregation.

        Returns:
            dict: Aggregated metadata counts.
        """
        if ignored_edges is None:
            ignored_edges = []
        nested_dict: Callable[[], defaultdict[str, defaultdict]] = lambda: defaultdict(defaultdict)

        count_resource_types = self.count_resource_types()

        summary = nested_dict()

        for resource_type in count_resource_types:

            resources = self.all_resources(resource_type)

            for _ in resources:

                if "count" not in summary[resource_type]:
                    summary[resource_type]["count"] = 0
                summary[resource_type]["count"] += 1

                # refs = nested_lookup("reference", _)
                refs = find_key_with_path(_, "reference", ignored_keys=ignored_edges)

                # if _['resourceType'] == 'Group':
                #     print(_)
                #     print(refs)

                for match in refs:
                    path, ref = match.values()

                    # A codeable reference is an object with a codeable concept and a reference
                    if isinstance(ref, dict):
                        ref = ref["reference"]
                    ref_resource_type = ref.split("/")[0]
                    if "references" not in summary[resource_type]:
                        summary[resource_type]["references"] = nested_dict()

                    # # only count references to resources that are not ResearchStudy
                    # if ref_resource_type == 'ResearchStudy' and resource_type != 'ResearchSubject':
                    #     continue
                    # if set([resource_type, ref_resource_type]) == set(['Group', 'Patient']):
                    #     pass

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
            db_path (Optional[str]): Path to the SQLite database file. Defaults to a temporary file.
            debug (Optional[bool]): Enable debug mode if True. Defaults to False.
        """
        if not db_path:
            # initializes the ResourceDB to a temporary file
            db_path = tempfile.NamedTemporaryFile(delete=False).name

        super().__init__(db_path)
        self.fhir_base_url = fhir_base_url
        self.max_requests = 10
        self.debug = debug
        self.recurse_count = 0

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

    async def execute_query(self, query_url: str, spinner: Halo = None, page_count: int = 0) -> list[dict[str, Any]]:
        """
        Executes a FHIR query for a given URL, returns all pages as a list of resources.

        Args:
            query_url (str): Fully constructed query URL.
            spinner (Halo): Spinner object to show progress. Defaults to None.
            page_count (int): The current page count. Defaults to 0.

        Returns:
            list: A list of resources from the query result.
        """
        retry = 0
        max_retry = 3
        log_every_n_pages = 10
        while retry < max_retry:
            async with httpx.AsyncClient() as client:
                try:
                    if self.debug:
                        print(f"Querying: {query_url}")
                    response = await client.get(query_url, timeout=300)
                    response.raise_for_status()
                    page_count += 1
                    query_result = response.json()
                    resources = []
                    next_link = [link for link in query_result.get("link", []) if link["relation"] == "next"]
                    entries = query_result.get("entry", [])
                    if len(entries) == 0:
                        logging.info(f"No entries: {query_result}")
                    for entry in entries:
                        # write to db
                        self.add(entry["resource"])
                        # return to caller
                        resources.append(entry["resource"])
                    if next_link:
                        if spinner and page_count % log_every_n_pages == 0:
                            estimated_number_of_pages = "unknown"
                            resource_type = "unknown"
                            if entries:
                                resource_type = entries[0]["resource"]["resourceType"]
                            if "total" in query_result:
                                estimated_number_of_pages = query_result["total"] // len(entries)
                            spinner.info(f"Fetching {resource_type} page {page_count} of {estimated_number_of_pages}")
                        for entry in await self.execute_query(next_link[0]["url"], spinner=spinner, page_count=page_count):
                            resources.append(entry)

                    if self.debug:
                        print(f"Query result: {len(resources)}")
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
                except httpx.RemoteProtocolError as e:
                    if retry == max_retry:
                        logging.warning(f"RemoteProtocolError: {e} sleeping for 5 seconds. Retry: {retry}")
                    await asyncio.sleep(5)
                    retry += 1
                except httpx.HTTPStatusError as e:
                    err: httpx.HTTPStatusError = e
                    if err.response.status_code in [503, 429]:
                        if retry == max_retry:
                            if self.debug:
                                print(f"RemoteProtocolError: {e} sleeping for 5 seconds. Retry: {retry}")
                            logging.warning(f"RemoteProtocolError: {e} sleeping for 5 seconds. Retry: {retry}")
                        await asyncio.sleep(5)
                        retry += 1
                    else:
                        retry = max_retry
                        logging.warning(f"RemoteProtocolError: {e} abandoning thread for url {query_url}")

        return []

    async def process_link(self, link, parent_resources, visited, spinner):
        """
        Processes a single link in the GraphDefinition.

        Args:
            link (dict): The link to process.
            parent_resources (list): List of parent resources.
            visited (set): Set of visited node-resource combinations to prevent cycles.
            spinner (Halo): Spinner object to show progress.
        """
        target_id = link["targetId"]
        source_id = link["sourceId"]
        if "params" in link:
            params = link["params"]
            current_path = set()
            for _ in parent_resources:
                if _["resourceType"] == source_id:
                    key = (_["resourceType"], _["id"], target_id)
                    if key not in visited:
                        visited.add(key)
                        parent = dotty({_["resourceType"]: _})
                        assert "path" in link, f"Path is required for {link}"
                        path = link["path"]
                        if path not in parent:
                            continue
                        _path = parent[path]
                        if path.endswith(".id"):
                            _path = source_id + "/" + _path
                        # See https://www.hl7.org/fhir/graphdefinition-definitions.html#GraphDefinition.link.params
                        if "_id={ref}" in params and "/" in _path:
                            _path = _path.split("/")[-1]
                        current_path.add(_path)
            if not current_path:
                if spinner:
                    spinner.fail(f"Could not find any resources for {source_id}->{target_id} link: {link}")
                return
            if spinner:
                spinner.info(
                    text=f"Processing link: {link['targetId']}/{link['params']} with {len(current_path)} {link['sourceId']}(s)"
                )
            _current_path = list(current_path)
            chunk_size = 40
            chunks = [_current_path]
            if len(_current_path) > chunk_size:
                chunks = [_current_path[i : i + chunk_size] for i in range(0, len(_current_path), chunk_size)]
            tasks = []
            for chunk in chunks:
                # https://www.hl7.org/fhir/graphdefinition-definitions.html#GraphDefinition.link.params
                _params = params.replace("{ref}", ",".join(chunk))
                query_url = f"{self.fhir_base_url}/{target_id}?{_params}"
                tasks.append(asyncio.create_task(self.execute_query(query_url, spinner=spinner)))
                if len(tasks) >= self.max_requests:
                    await asyncio.gather(*tasks)
                    tasks = []
            await asyncio.gather(*tasks)
        else:
            logging.debug(f"No `params` property found in link. {link} continuing")

        if spinner:
            spinner.clear()
            spinner.succeed(f"Processed link: {link['targetId']}/{link.get('params', '')}")

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
            parent_resources (list): Resources returned from the last query, can have multiple resource types.
            graph_definition (dict): The entire GraphDefinition resource.
            visited (set): Set of visited node-resource combinations to prevent cycles.
            spinner (Halo): Spinner object to show progress.
        """
        links = [link for link in graph_definition.get("link", []) if link.get("sourceId") == parent_target_id]
        if spinner:
            spinner.info(f"Processing {len(links)} links for {parent_target_id} in parallel.")
        tasks = [self.process_link(link, parent_resources, visited, spinner) for link in links]
        await asyncio.gather(*tasks)

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
        """
        visited: set[tuple[Any, Any, Any]] = set()

        if path:
            url = self.fhir_base_url + path
            assert self.recurse_count == 0, "Should not call this twice"
            if spinner:
                spinner.info(text=f"Fetching {url}")
            self.recurse_count += 1
            parent_resources = await self.execute_query(url, spinner=spinner)
            if spinner:
                spinner.clear()
        else:
            parent_resources = []

        if len(parent_resources) == 0:
            if spinner:
                spinner.fail("No resources found")

        parent_resource_types = self.count_resource_types().keys()
        processed_links = []
        while True:
            parallelize = defaultdict(list)
            for link in graph_definition["link"]:
                if link in processed_links:
                    continue
                if link["sourceId"] in parent_resource_types:
                    processed_links.append(link)
                    parallelize[link["sourceId"]].append(link)

            if not parallelize:
                if self.debug:
                    print("No more links to process", parent_resource_types)
                break

            tasks = []
            for source_id, links in parallelize.items():
                if spinner:
                    spinner.info(text=f"Processing {source_id} with {len(parent_resources)} resources")

                # create a sub graph definition for the with only the links for the current source_id
                parent_resources = self.all_resources(source_id)
                _sub_graph_definition = {"link": links}

                tasks.append(
                    self.process_links(
                        parent_target_id=source_id,
                        parent_resources=parent_resources,
                        graph_definition=_sub_graph_definition,
                        visited=visited,
                        spinner=spinner,
                    )
                )

            await asyncio.gather(*tasks)
            parent_resource_types = self.count_resource_types().keys()


def tree() -> defaultdict:
    """A recursive defaultdict."""
    return defaultdict(tree)


class VocabularyRunner:
    """
    A class to fetch and collect vocabularies from a FHIR server.

    Args:
        fhir_base_url (str): Base URL of the FHIR server.
    """

    def __init__(self, fhir_base_url: str):
        """
        Initialize the VocabularyRunner instance.

        Args:
            fhir_base_url (str): Base URL of the FHIR server.
        """
        self.fhir_base_url = fhir_base_url

    async def fetch_resource(self, resource_type: str, spinner: Halo = None) -> dict[str, dict[Any, Any]]:
        """
        Fetch resources of a given type from the FHIR server.

        Args:
            resource_type (str): The type of resource to fetch.
            spinner (Halo, optional): A Halo spinner object to show progress. Defaults to None.

        Returns:
            dict: A dictionary with resource type as keys and counts as values.
        """
        counts: dict = {resource_type: {}}
        category_counts = counts[resource_type]
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

        Args:
            resource_types (list[str]): A list of resource types to collect vocabularies from.
            spinner (Halo, optional): A Halo spinner object to show progress. Defaults to None.

        Returns:
            list: A list of dictionaries with resource type as keys and counts as values.
        """
        tasks = []

        for resource_type in resource_types:
            tasks.append(asyncio.create_task(self.fetch_resource(resource_type, spinner)))

        results = await asyncio.gather(*tasks)
        return [_ for _ in results]


def find_key_with_path(data, key_to_find, ignored_keys=None):
    """
    Traverse the dictionary and find all occurrences of a given key.
    Returns a list of dictionaries containing the path and value for each match.
    Paths containing keys in the ignored_keys list are skipped.

    :param data: The input dictionary or list to search.
    :param key_to_find: The key to look for in the data structure.
    :param ignored_keys: A list of keys to ignore during traversal.
    :return: A list of dictionaries with 'path' and 'value' for each match.
    """
    if ignored_keys is None:
        ignored_keys = []

    results = []

    def recursive_search(d, current_path=None):
        if current_path is None:
            current_path = []
        if isinstance(d, dict):
            for key, value in d.items():
                new_path = current_path + [key]

                # if data.get('resourceType', None) == 'Group':
                #     print('new_path scalar', new_path, data[new_path[0]])

                if key in ignored_keys:
                    continue  # Skip paths containing ignored keys

                if key == key_to_find:
                    found_ignored_key_in_extension = False
                    if "extension" in new_path:
                        extension_url = get_value_from_path(data, new_path[:-2] + ["url"])
                        if extension_url:
                            for k in ignored_keys:
                                if k in extension_url:
                                    found_ignored_key_in_extension = True
                                    break
                    if found_ignored_key_in_extension:
                        continue
                    results.append({"path": new_path, "value": value})
                recursive_search(value, new_path)
        elif isinstance(d, list):
            for index, item in enumerate(d):
                new_path = current_path + [index]
                # if data.get('resourceType', None) == 'Group':
                #     print('new_path []', new_path)
                recursive_search(item, new_path)

    recursive_search(data)
    return results


def get_value_from_path(data, path):
    """
    Retrieve a value from a nested dictionary or list using a path array.

    :param data: The nested dictionary or list to retrieve the value from.
    :param path: A list representing the path to the desired value.
    :return: The value at the specified path, or None if the path is invalid.
    """
    try:
        for key in path:
            data = data[key]
        return data
    except (KeyError, IndexError, TypeError):
        return None
