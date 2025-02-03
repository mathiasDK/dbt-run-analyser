import json

def manifest_parser(path_to_manifest)->dict:
    # Convert json to dict
    d = json.load(open(path_to_manifest))

    nodes = {}

    for node, vals in d["nodes"].items():
        upstream_models = vals.get("depends_on")["nodes"]
        if upstream_models == []:
            nodes[node] = None
        else:
            nodes[node] = upstream_models

    return nodes
