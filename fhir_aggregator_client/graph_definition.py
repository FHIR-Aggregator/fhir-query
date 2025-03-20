import importlib.resources as pkg_resources
import os

import yaml


def get_installed_graph_descriptions_path():
    package_name = "fhir_aggregator_client"
    resource_path = "graph-definitions"
    try:
        resource = pkg_resources.files(package_name).joinpath(resource_path)
        if resource.is_dir():
            return str(resource)
        else:
            raise FileNotFoundError(f"{resource_path} directory not found in the installed package.")
    except Exception as e:
        raise RuntimeError(f"Error locating {resource_path} directory: {e}")


def ls_yaml_files(local_path):
    """
    Recursively print the full path names of YAML files in the given local path.

    Args:
        local_path (str): The local directory path to search for YAML files.
    """
    _files = []
    for root, dirs, files in os.walk(local_path):
        for file in files:
            if file.endswith((".yaml", ".yml")):
                _files.append(os.path.join(root, file))
    return _files


# def copy_graph_definitions(local_path):
#     """Copy the graph definitions from the FHIR Aggregator package to the local path."""
#     graph_descriptions_path = get_installed_graph_descriptions_path()
#     os.makedirs(local_path)
#     for item in os.listdir(graph_descriptions_path):
#         s = os.path.join(graph_descriptions_path, item)
#         d = os.path.join(local_path, item)
#         if os.path.isdir(s):
#             shutil.copytree(s, d, dirs_exist_ok=True)
#         else:
#             shutil.copy2(s, d)


def ls() -> list[dict]:
    """List the graph definitions from the FHIR Aggregator package."""
    paths = ls_yaml_files(get_installed_graph_descriptions_path())

    def _dict(_path) -> dict:
        with open(_path, "r") as file:
            return yaml.safe_load(file)

    summaries = []
    for path in paths:
        graph_definition_dict = _dict(path)
        summaries.append(
            {
                "description": graph_definition_dict.get("description", "No description found"),
                "path": path,
                # "name": graph_definition_dict.get("name", "No name found"),
                "id": graph_definition_dict.get("id"),
            }
        )
    return summaries
