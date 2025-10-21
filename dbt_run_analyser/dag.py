from .node import Node
from .utils.manifest_parser import manifest_parser
from .utils.log_parser import LogParser
import polars as pl


class DAG:
    """
    A class representing a Directed Acyclic Graph (DAG) for dbt models.

    Attributes:
        nodes (dict): Dictionary of nodes in the DAG.
        node_children (dict): Dictionary of node children.
        node_parents (dict): Dictionary of node parents.
        _run_time_lookup (dict): Dictionary of model run times.
        df (pl.DataFrame): DataFrame containing model run times.
    """

    def __init__(self, manifest_path: str = None, log_file: str = None):
        """
        Initializes the DAG with optional manifest and log files.

        Args:
            manifest_path (str, optional): Path to the dbt manifest file.
            log_file (str, optional): Path to the log file.
        """
        self.nodes = {}
        self.node_children = {}
        self.node_parents = {}
        self._run_time_lookup = {}
        self.df = pl.DataFrame()

        if manifest_path:
            self.manifest_to_nodes(manifest_path)

        if log_file:
            self.log_to_run_time(log_file)

    def add_node(self, node: Node) -> None:
        """
        Adds a node to the DAG.

        Args:
            node (Node): Node to add.
        """
        if node.name in self.nodes.keys():
            print(
                "The node with this name already exists. Remove it before adding it again."
            )
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

    def bulk_add_nodes(self, nodes: dict) -> None:
        """
        Adds multiple nodes to the DAG.

        Args:
            nodes (dict): Dictionary of nodes to add.
        """
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

    def remove_node(self, node: Node) -> None:
        """
        Removes a node from the DAG.

        Args:
            node (Node): Node to remove.
        """
        if node.name not in self.nodes.keys():
            print("The node does not exist. Add it through the add_note() method.")
            return None
        del self.nodes[node.name]
        del self.node_parents[node.name]
        if node.parents is not None:
            for parent in node.parents:
                self.node_children[parent].remove(node.name)

    # def get_children_names(self, table_name: str) -> list[str]:
    #     return self.node_children[table_name]

    # def get_parent_names(self, table_name: str) -> list[str]:
    #     return self.node_parents[table_name]

    def get_upstream_dependencies(self, table_name: str, deps: list[str] = []):
        """
        Gets the upstream dependencies of a node.

        Args:
            table_name (str): Name of the node.
            deps (list[str], optional): List of dependencies.

        Returns:
            list[str]: List of upstream dependencies.
        """
        parents = self.node_parents[table_name]
        if parents is not None:
            for parent_name in self.node_parents[table_name]:
                if parent_name not in deps:
                    deps.append(parent_name)
                    deps.extend(self.get_upstream_dependencies(parent_name, deps=deps))
        return list(set(deps))  # ensures uniqueness
    
    def get_downstream_dependencies(self, table_name: str, deps: list[str] = None):
        """
        Gets the downstream dependencies of a node.

        Args:
            table_name (str): Name of the node.
            deps (list[str], optional): List of dependencies.

        Returns:
            list[str]: List of downstream dependencies.
        """
        deps = []
        if table_name in self.node_children.keys():
            children = self.node_children[table_name]
            if children is not None:
                for children_name in self.node_children[table_name]:
                    if children_name not in deps:
                        deps.append(children_name)
                        deps.extend(self.get_downstream_dependencies(children_name, deps=deps))
        return list(set(deps))  # ensures uniqueness
    
    def get_all_end_nodes(self):
        all_models = set(self.nodes)
        models_with_downstream = set(self.node_children)
        models_without_downstream = all_models - models_with_downstream
        return models_without_downstream

    def find_all_paths_to_node(self, target, path=None, paths=[]):
        """
        Finds all paths to a target node.

        Args:
            target (str): Target node.
            path (list[str], optional): Current path.
            paths (list[list[str]], optional): List of paths.

        Returns:
            list[list[str]]: List of paths to the target node.
        """
        if path is None:
            path = []

        # Add the target node to the current path
        path = [target] + path

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

    def get_critial_paths(self, model=None):
        """
        Gets the paths to the model sorted by run time.

        Args:
            model (str, optional): Name of the model.

        Returns:
            dict: A sorted dictionary of paths.
        """
        if model is None:
            return None

        paths = self.find_all_paths_to_node(model)

        output = {}
        for path in paths:
            total_run_time = sum(self.get_run_time(node) for node in path)
            run_time_dict = {node: self.get_run_time(node) for node in path}
            run_time_dict = {
                k: v
                for k, v in sorted(
                    run_time_dict.items(), key=lambda item: item[1], reverse=True
                )
            }
            path_string = " ".join(path)
            output[path_string] = {
                "path": path,
                "total_run_time": total_run_time,
                "run_time_dict": run_time_dict,
            }
        output = {
            k: v
            for k, v in sorted(
                output.items(), key=lambda item: item[1]["total_run_time"], reverse=True
            )
        }

        return output

    def get_critial_path(self, model=None):
        """
        Gets the critical path for a model, i.e. the longest path to the model.

        Args:
            model (str, optional): Name of the model.

        Returns:
            dict: Dictionary containing the critical path.
        """
        for path, v in self.get_critial_paths(model).items():
            break
        return {path: v}

    def get_inbetween_models(self, model=None):
        """
        Gets the models that exist between others.

        Args:
            model (str, optional): Name of the model.
        """
        # check if a model exists in between others, e.g. a->b->c, a->c should highlight b.
        pass

    def get_run_time(self, model) -> float:
        """
        Gets the run time of a model.

        Args:
            model (str): Name of the model.

        Returns:
            float: Run time of the model.
        """
        run_time = self._run_time_lookup.get(model)
        if run_time is None:
            print(f"No runtime for {model}")
            return 0
        return run_time

    def manifest_to_nodes(self, manifest_path: str) -> None:
        """
        Converts a manifest file to nodes and adds them to the DAG.

        Args:
            manifest_path (str): Path to the manifest file.
        """
        nodes = manifest_parser(manifest_path)
        for node, parents in nodes.items():
            self.add_node(Node(name=node, parents=parents))

    def log_to_run_time(self, log_file: str) -> None:
        """
        Parses a log file and updates the run times of models.

        Args:
            log_file (str): Path to the log file.
        """
        df = LogParser(log_file).parse_logs()
        run_time = df[["model_name", "run_time"]].to_dict(as_series=False)
        for model, run_time in zip(run_time["model_name"], run_time["run_time"]):
            self._run_time_lookup[model] = run_time  # overwrites existing runtimes
        self.df = df

    def _estimate_thread(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Estimates the thread for each model run.

        Args:
            df (pl.DataFrame): DataFrame containing model run times.

        Returns:
            pl.DataFrame: DataFrame with estimated threads.
        """
        df = df.sort(by=["relative_start_time", "relative_end_time"])
        df = df.with_columns(thread=pl.lit(0))

        parellel_processing = {
            k: {"end_time": None} for k in range(200)
        }  # Setting an arbitrary max number of threads
        d = df.rows_by_key(key=["model_name"], named=True)

        for idx, (model_name, row) in enumerate(d.items()):
            row = row[0]
            for m, v in parellel_processing.items():
                if (
                    v["end_time"] is None
                    or v.get("end_time") < row["relative_start_time"]
                ):
                    parellel_processing[m] = {"end_time": row["relative_end_time"]}
                    df[idx, "thread"] = m
                    break
        return df

    def to_df(self, critical_path_model: str = None) -> pl.DataFrame:
        """
        Converts the DAG to a DataFrame.

        Args:
            critical_path_model (str, optional): Name of the critical path model.

        Returns:
            pl.DataFrame: DataFrame containing model run times.
        """
        nodes = None
        if len(self.df) == 0:
            raise ValueError("No logs found. Please provide a log file.")
        if critical_path_model:
            critical_path_model = self.get_critial_path(critical_path_model)
            first_model = list(critical_path_model.keys())[0]
            nodes = critical_path_model.get(first_model).get("path")

        if nodes is not None:
            df = self.df.filter(pl.col("model_name").is_in(nodes))
            # Reset the relative start time
            first_start_time = df["relative_start_time"].min()
            df = df.with_columns(
                (pl.col("relative_start_time") - first_start_time).alias(
                    "relative_start_time"
                ),
                (pl.col("relative_end_time") - first_start_time).alias(
                    "relative_end_time"
                ),
            )
            # Should relative start time be the previous relative end time?
        else:
            df = self.df

        df = self._estimate_thread(df)
        return df

    def get_thread_utilisation(self) -> float:
        """
        Calculates how much the threads are being used, i.e. how well the run is being parallelised.

        If a single model runs while no other models run it will return a low score, which is an indication of
        a possibility for splitting that single long running model into multiple smaller models to better utilise
        the threads.

        Returns:
            float: The utilisation %.
        """
        if "thread" not in self.df.columns:
            self.df = self._estimate_thread(self.df)

        # Calculating the total run time capacity if it is perfectly parallellised
        n_threads = self.df["thread"].max() + 1  # +1 since it starts at 0
        complete_time = self.df["relative_end_time"].max()
        capacity = n_threads * complete_time

        # Calculating the actual run time
        run_time = (
            self.df["run_time"].round(0).sum()
        )  # ensuring that a run time of 3.99 seconds is converted to 4 seconds

        # Utilisation is the run time divided by capacity
        utilisation = run_time * 1.0 / capacity

        return utilisation
