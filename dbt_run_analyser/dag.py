from .node import Node
from .utils.manifest_parser import manifest_parser
from .utils.log_parser import LogParser

class DAG:
    def __init__(self, manifest_path:str=None, log_file:str=None):
        self.nodes = {}
        self.node_children = {}
        self.node_parents = {}
        self._run_time_lookup = {}

        if manifest_path:
            self.manifest_to_nodes(manifest_path)

        if log_file:
            self.log_to_run_time(log_file)

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
    
    def find_all_paths_to_node(self, target, path=None, paths=[]):
        if path is None:
            path = []
        
        # Add the target node to the current path
        path = [target] + path

        print("target:", target)
        print("path", path)
        
        # If the target node has no incoming edges, return the current path
        if target not in self.node_parents:
            return [path]
        
        paths = []
        
        # Traverse each predecessor of the target node
        if self.node_parents[target] is None:
            return [path]
        for node in self.node_parents[target]:
            if node is None:
                return paths
            if node not in path:  # Avoid cycles
                new_paths = self.find_all_paths_to_node(node, path, paths)
                for p in new_paths:
                    paths.append(p)
                    
        return paths
    
    def get_critial_path(self, model=None):
        if model is None:
            return None
        
        paths = self.find_all_paths_to_node(model)
        
        output = {}
        for path in paths:
            total_run_time = sum(self.get_run_time(node) for node in path)
            run_time_dict = {node: self.get_run_time(node) for node in path}
            run_time_dict = {k: v for k, v in sorted(run_time_dict.items(), key=lambda item: item[1], reverse=True)}
            output[path[0]] = {
                'path': path,
                'total_run_time': total_run_time,
                'run_time_dict': run_time_dict
            }
        output = {k: v for k, v in sorted(output.items(), key=lambda item: item[1]['total_run_time'], reverse=True)}
        
        return output

    def get_inbetween_models(self, model=None):
        # check if a model exists in between others, e.g. a->b->c, a->c should highlight b.
        pass

    def get_run_time(self, model)->float:
        run_time = self._run_time_lookup.get(model)
        if run_time is None:
            print(f"No runtime for {model}")
            return None
        return run_time
    
    def manifest_to_nodes(self, manifest_path:str)->None:
        nodes = manifest_parser(manifest_path)
        for node, parents in nodes.items():
            self.add_node(Node(name=node, parents=parents))

    def log_to_run_time(self, log_file:str)->None:
        df = LogParser(log_file).parse_logs()
        run_time = df[['model_name', 'run_time']].to_dict(as_series=False)
        for model, run_time in zip(run_time['model_name'], run_time['run_time']):
            self._run_time_lookup[model] = run_time # overwrites existing runtimes
    