# Initialize the fhir_query package
import asyncio
import json
import logging
import sqlite3
import sys
import tempfile
from collections import defaultdict
from typing import Generator, Any, Optional

import httpx
from dotty_dict import dotty
from halo import Halo


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
                    resource JSON NOT NULL,
                    PRIMARY KEY (id, resource_type)
                )
            """
            )

    def add(self, resource: dict[str, Any]) -> None:
        """
        Add a resource to the 'resources' table.
        :param resource: A dictionary with 'id', 'resourceType', and other fields.
        """
        if "id" not in resource or "resourceType" not in resource:
            raise ValueError("Resource must contain 'id' and 'resourceType' fields.")

        try:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO resources (id, resource_type, resource)
                    VALUES (?, ?, ?)
                """,
                    (resource["id"], resource["resourceType"], json.dumps(resource)),
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


class GraphDefinitionRunner(ResourceDB):
    """
    A class to parse a FHIR GraphDefinition and execute the queries defined in its links.

    See
    https://www.devdays.com/wp-content/uploads/2021/12/Rene-Spronk-GraphDefinition-_-DevDays-2019-Amsterdam-1.pdf
    """

    def __init__(self, fhir_base_url: str, db_path: Optional[str] = None):
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
        Executes a FHIR query for a given URL.

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
                    response = await client.get(query_url)
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
                            _path = parent[path]
                            if path.endswith(".id"):
                                _path = source_id + "/" + _path
                            current_path.add(_path)
                assert (
                    current_path
                ), f"Could not find any resources for {source_id} link: {link}\nparent_resources: {parent_resources}\n visited: {visited}"

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
        start_resource_type: str,
        start_resource_id: str,
        spinner: Halo,
    ) -> None:
        """
        Runs the GraphDefinition queries starting from the specified resource.

        Args:
            graph_definition (dict): The GraphDefinition resource.
            start_resource_type (str): ID of the starting node in the GraphDefinition.
            start_resource_id (str): ID of the starting resource.
            spinner (Halo): Spinner object to show progress.

        Returns:
            dict: Aggregated results of all traversed resources.
        """
        visited: set[tuple[Any, Any, Any]] = set()
        parent_resources = [{"resourceType": start_resource_type, "id": start_resource_id}]
        return await self.process_links(
            parent_resources=parent_resources,
            parent_target_id=start_resource_type,
            graph_definition=graph_definition,
            visited=visited,
            spinner=spinner,
        )
