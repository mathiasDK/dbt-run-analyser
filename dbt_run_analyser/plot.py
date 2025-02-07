from .dag import DAG

class ShowDBTRun(DAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def plot(self):
        pass

