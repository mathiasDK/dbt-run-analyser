from .node import Node
from collections import defaultdict

class DAG:
    def __init__(self):
        self.nodes = {}
        self.node_children = {}
        self.node_parents = {}
        self._run_time_lookup = {}

    def add_node(self, node:Node)->None:
        if node.name in self.nodes.keys():
            print("The node with this name already exists. Remove it before adding it again.")
            return None
        self.nodes[node.name] = node
        self.node_parents[node.name] = node.parents
        if node.run_time:
            self._run_time_lookup[node.name] = node.run_time
        if node.parents is not None:
            for parent in node.parents:
                if parent not in self.node_children.keys():
                    self.node_children[parent] = [node.name]
                else:
                    self.node_children[parent].append(node.name)

    def bulk_add_nodes(self, nodes:dict)->None:
        self.nodes.update(nodes)
        for node_name, node in nodes.items():
            if node.run_time:
                self._run_time_lookup[node.name] = node.run_time
                
            if node_name not in self.node_parents:
                if node.parents is None:
                    self.node_parents[node_name] = None
                else:
                    self.node_parents[node_name] = node.parents
            else:
                self.node_parents[node_name].update(node.parents)
            
            if node.parents is not None:
                for parent in node.parents:
                    if parent not in self.node_children:
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
    
    def get_critial_path(self, model=None):
        # if model is none then set it to the model which completes last
        # otherwise look at all upstream models and their run time.
        results = defaultdict(list)
        print(self._run_time_lookup)

        def traverse(node=model, current_path="", current_sum=0):
            # if node not in self.node_parents or node is None:
            #     return
            
            run_time = self.get_run_time(node)
            parents = self.node_parents.get(node)

            # Opdater nuværende sti og sum
            new_path = f"{node}->{current_path}" if current_path else node
            new_sum = current_sum + run_time

            # Gem stien og summen
            results[node].append({"path": new_path, "total_run_time": new_sum})

            # Hvis der er forældre, gå rekursivt opad for hver parent
            if parents:
                for parent in parents:
                    traverse(parent, new_path.split("->"), new_sum)

        # Start traversal for hver node uden forældre
        for model in self.node_parents:
            if self.node_parents[model] is not None:  # Ingen forældre = en rod i træet
                traverse(model, [], 0)

        return results

    def get_inbetween_models(self, model=None):
        # check if a model exists in between others, e.g. a->b->c, a->c should highlight b.
        pass

    def get_run_time(self, model)->float:
        run_time = self._run_time_lookup.get(model)
        if run_time is None:
            print(f"No runtime for {model}")
            return None
        return run_time