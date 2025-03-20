from pyvis.network import Network

from fhir_aggregator_client import ResourceDB


def _container():
    """Create a pyvis container."""
    return Network(notebook=True, cdn_resources="in_line")  # filter_menu=True, select_menu=True


def _load(net: Network, aggregation: dict) -> Network:
    """Load the aggregation into the visualization network."""
    # add vertices
    for resource_type, _ in aggregation.items():
        assert "count" in _, _
        net.add_node(resource_type, label=f"{resource_type}/{_['count']}")
    # add edges
    for resource_type, _ in aggregation.items():
        for ref in _.get("references", {}):
            count = _["references"][ref]["count"]
            if resource_type not in net.get_nodes():
                net.add_node(resource_type, label=f"{resource_type}/?")
            if ref not in net.get_nodes():
                net.add_node(ref, label=f"{ref}/?")
            net.add_edge(resource_type, ref, title=count, value=count)
    return net


def visualize_aggregation(aggregation: dict, output_path: str) -> None:
    """Visualize the aggregation."""
    # Load it into a pyvis
    net = _load(_container(), aggregation)
    net.show_buttons(filter_=["physics"])
    net.save_graph(str(output_path))


def create_network_graph(db_path: str, output_path: str) -> None:
    """Render metadata as a network graph into output_path.

    \b
    db_path: The directory path to the db.
    output_path: The path to save the network graph.
    """
    db = ResourceDB(db_path=db_path)
    visualize_aggregation(db.aggregate(), output_path)
