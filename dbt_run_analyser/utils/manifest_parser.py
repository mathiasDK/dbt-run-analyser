import json

def manifest_parser(path_to_manifest)->dict:
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
