class ManifestParser:
    def __init__(self) -> None:
        self.nodes = None

    @property
    def nodes(self):
        return self.nodes
    
    @nodes.setter
    def nodes(self, nodes: list[str]):
        self.nodes = nodes

    @nodes.deleter
    def nodes(self):
        print("Removing all nodes")
        del self.nodes