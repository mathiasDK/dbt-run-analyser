class Node:
    def __init__(self, name:str, run_time:float=None, parents:list[str]=None, children:list[str]=None):
        self.name = name
        self.run_time = run_time
        self.parents = parents
        self.children = children

    # @property
    # def name(self):
    #     return self.name
    
    # @name.setter
    # def name(self, name):
    #     self.name = name

    # @name.deleter
    # def name(self):
    #     raise ValueError("A node must have a name.")

    # @property
    # def run_time(self):
    #     return self.run_time
    
    # @run_time.setter
    # def run_time(self, run_time):
    #     self.run_time = run_time

    # @run_time.deleter
    # def run_time(self):
    #     print(f"{self.name} should have a run_time.")
    #     del self.run_time

    # @property
    # def parents(self):
    #     return self.parents
    
    # @parents.setter
    # def parents(self, parents):
    #     self.parents = parents

    # @parents.deleter
    # def parents(self):
    #     print(f"{self.name} has no upstream dependencies anymore.")
    #     del self.parents
        
    # @property
    # def children(self):
    #     return self.children
    
    # @children.setter
    # def children(self, children):
    #     self.children = children

    # @children.deleter
    # def children(self):
    #     del self.children