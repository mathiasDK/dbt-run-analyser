from .node import Node

class DAG:
    def __init__(self):
        pass
        self.nodes = {}

    def add_node(self, node:Node)->None:
        self.nodes[node.name] = node

    def get_children_names(self, node:Node)->list[str]:
        return node.children
    
    def get_parent_names(self, node:Node)->list[str]:
        return node.parents
        
    def get_upstream_dependencies(self, node:Node):
        for parent_name in node.parents:
            parent = self.nodes[parent_name]
            if parent is not None:
                yield from self.get_upstream_dependencies(parent)
            else:
                break