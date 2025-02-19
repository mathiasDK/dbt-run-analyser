class Node:
    """
    A class representing a node in a DAG.

    Attributes:
        name (str): Name of the node.
        run_time (float): Run time of the node.
        parents (list[str]): List of parent nodes.
        children (list[str]): List of child nodes.
    """

    def __init__(
        self,
        name: str,
        run_time: float = 0,
        parents: list[str] = None,
        children: list[str] = None,
    ):
        """
        Initializes the Node with the given attributes.

        Args:
            name (str): Name of the node.
            run_time (float, optional): Run time of the node.
            parents (list[str], optional): List of parent nodes.
            children (list[str], optional): List of child nodes.
        """
        self.name = name
        self.run_time = run_time
        self.parents = parents
        self.children = children

    # @property
    # def run_time(self):
    #     return self.run_time

    # @run_time.setter
    # def run_time(self, run_time:float):
    #     self.run_time = run_time

    # @run_time.deleter
    # def run_time(self):
    #     raise AttributeError("You must have a run time for a node")
