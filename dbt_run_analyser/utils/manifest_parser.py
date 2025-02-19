import json


def manifest_parser(path_to_manifest) -> dict:
    """
    Parses a dbt manifest file and returns a dictionary of model dependencies.

    Args:
        path_to_manifest (str): Path to the dbt manifest file.

    Returns:
        dict: A dictionary where keys are model names and values are lists of upstream model names.
    """
    # Convert json to dict
    d = json.load(open(path_to_manifest))

    nodes = {}
    name_lookup = {}

    for node, vals in d["nodes"].items():
        node_name = vals.get("name")
        name_lookup[node] = node_name
        upstream_models = vals.get("depends_on")["nodes"]
        if upstream_models == []:
            nodes[node_name] = None
        else:
            nodes[node_name] = upstream_models

    for node, parents in nodes.items():
        if parents is not None:
            pretty_names = []
            for parent in parents:
                pretty_names.append(name_lookup.get(parent, parent))
            nodes[node] = pretty_names

    return nodes
