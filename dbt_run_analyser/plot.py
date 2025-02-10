from .dag import DAG
import plotly.graph_objects as go

class ShowDBTRun(DAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.figure = go.Figure

    def plot_run_time(self, title:str=None, run_time_starting_point: int|float=0, run_time_highlight: int|float=1e6, run_time_show_model_name: int|float=1e6):
        if len(self.df)==0:
            raise Exception("You must add data before you can plot.")
        
        for row in self.df.iter_rows(named=True):
            start = row["relative_start_time"].total_seconds()
            if start >= run_time_starting_point:
                end = row["relative_end_time"].total_seconds()
                thread = row["thread"]
                fillcolor = "grey" if row["run_time"].total_seconds()<run_time_highlight else "red"
                model_name = "" if row["run_time"].total_seconds()<run_time_show_model_name else row["model_name"]
                self.__add_run_time(thread=thread, start=start, end=end, fillcolor=fillcolor, model_name=model_name)
        
        # Layout
        self.figure.update_layout(
            template="simple_white",
            yaxis=dict(range=[-0.5, self.df["thread"].max()+0.5], title="Thread"),
            xaxis=dict(title="Run time (s)"),
            title=title
        )
        return self.figure

    def _add_run_time(self, thread, start, end, fillcolor, model_name:str="")->None:
        self.figure.add_shape(
            type="rect",
            x0=start,
            x1=end,
            y0=thread-0.35,
            y1=thread+0.35,
            fillcolor=fillcolor,
            label=dict(
                text=model_name, 
                font=dict(size=10),
            ),
        )