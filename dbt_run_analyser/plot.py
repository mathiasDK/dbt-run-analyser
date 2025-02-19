from .dag import DAG
import plotly.graph_objects as go

class ShowDBTRun(DAG):
    """
    A class to visualize dbt run times using Plotly.
    
    Inherits from the DAG class.
    
    Attributes:
        figure (go.Figure): Plotly figure object.
        df (pl.DataFrame): DataFrame containing model run times.
    """
    def __init__(self, *args, **kwargs):
        """
        Initializes the ShowDBTRun class.
        """
        super().__init__(*args, **kwargs)
        self.figure = go.Figure()
        self.df = self.to_df()

    def plot_run_time(self, title: str = None, run_time_starting_point: int | float = 0, run_time_highlight: int | float = 1e6, run_time_show_model_name: int | float = 0):
        """
        Plots the run times of models.
        
        Args:
            title (str, optional): Title of the plot.
            run_time_starting_point (int | float, optional): Starting point of the run time. If there are a lot of models it can take some time to plot. By not plotting models before a specific starting point you can save some time.
            run_time_highlight (int | float, optional): Threshold to highlight run times. If the model run time is greater than this value, it will be highlighted.
            run_time_show_model_name (int | float, optional): Threshold to show model names. If the model run time is greater than this value, the model name will be shown.
        
        Returns:
            go.Figure: Plotly figure object.
        """
        if len(self.df) == 0:
            raise Exception("You must add data before you can plot.")
        
        for row in self.df.iter_rows(named=True):
            start = row["relative_start_time"]
            if start >= run_time_starting_point:
                end = row["relative_end_time"]
                thread = row["thread"]
                fillcolor = "#c2c2c2" if row["run_time"] < run_time_highlight else "#D73809"
                show_model_name = True if row["run_time"] >= run_time_show_model_name else False
                model_name = row["model_name"]
                self._add_run_time(thread=thread, start=start, end=end, fillcolor=fillcolor, model_name=model_name, show_model_name=show_model_name)
        
        # Layout
        self.figure.update_layout(
            template="simple_white",
            yaxis=dict(range=[-0.5, self.df["thread"].max() + 0.5], title="Thread", type="category"),
            xaxis=dict(title="Run time (s)", range=[run_time_starting_point, self.df["relative_end_time"].max()]),
            title=title
        )
        return self.figure
    
    def plot_critical_path(self, critical_path_model: str, *args, **kwargs):
        """
        Plots the critical path of a model.
        
        Args:
            critical_path_model (str): Name of the critical path model.
        
        Returns:
            go.Figure: Plotly figure object.
        """
        self.plot_run_time(*args, **kwargs)
        d = self.get_critial_path(critical_path_model)
        for _, v in d.items():
            nodes = v["path"]
            break
        for node in nodes:
            self._highlight_node(node)
        return self.figure
    
    def _highlight_node(self, node: str) -> None:
        """
        Highlights a node in the plot.
        
        Args:
            node (str): Name of the node to highlight.
        """
        HIGHLIGHT_COLOR = "#940C08"
        
        self.figure.update_shapes(
            selector=dict(name=node),
            fillcolor=HIGHLIGHT_COLOR,
            label=dict(
                text=node, 
                font=dict(size=10, color="#ddd"),
            ),
            opacity=1
        )
        
    def _add_run_time(self, thread, start, end, fillcolor, model_name: str = "", show_model_name: bool = True) -> None:
        """
        Adds a run time rectangle to the plot.
        
        Args:
            thread (int): Thread number.
            start (float): Start time.
            end (float): End time.
            fillcolor (str): Fill color of the rectangle.
            model_name (str, optional): Name of the model.
            show_model_name (bool, optional): Whether to show the model name.
        """
        self.figure.add_shape(
            name=model_name,
            type="rect",
            x0=start,
            x1=end,
            y0=thread - 0.35,
            y1=thread + 0.35,
            fillcolor=fillcolor,
            opacity=1,
            label=dict(
                text=model_name if show_model_name else None, 
                font=dict(size=10),
            ),
        )