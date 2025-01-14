from .node import Node

class DAG:
    def __init__(self):
        pass
        self.nodes = {}
        self.node_children = {}
        self.node_parents = {}

    def add_node(self, node:Node)->None:
        if node.name in self.nodes.keys():
            print("The node with this name already exists. Remove it before adding it again.")
            return None
        self.nodes[node.name] = node
        self.node_parents[node.name] = node.parents
        if node.parents is not None:
            for parent in node.parents:
                if parent not in self.node_children.keys():
                    self.node_children[parent] = [node.name]
                else:
                    self.node_children[parent].append(node.name)

    def remove_node(self, node:Node)->None:
        if node.name not in self.nodes.keys():
            print("The node does not exist. Add it through the add_note() method.")
            return None
        del self.nodes[node.name]
        del self.node_parents[node.name]
        if node.parents is not None:
            for parent in node.parents:
                self.node_children[parent].remove(node.name)

    # def get_children_names(self, table_name: str)->list[str]:
    #     return self.node_children[table_name]
    
    # def get_parent_names(self, table_name: str)->list[str]:
    #     return self.node_parents[table_name]
        
    def get_upstream_dependencies(self, table_name:str, deps:list[str]=[]):
        parents = self.node_parents[table_name]
        if parents is not None:
            for parent_name in self.node_parents[table_name]:
                if parent_name not in deps:
                    deps.append(parent_name)
                    deps.extend(self.get_upstream_dependencies(parent_name, deps=deps))
        return list(set(deps)) # ensures uniqueness