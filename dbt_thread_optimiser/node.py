class Node:
    def __init__(self, name:str, run_time: float = 0, parents: list[str]=None, children: list[str]=None):
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