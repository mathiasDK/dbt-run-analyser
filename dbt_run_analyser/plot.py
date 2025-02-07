from .dag import DAG
import polars as pl

class ShowDBTRun(DAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _estimate_thread(self, df:pl.DataFrame)->pl.DataFrame:
        df = df.sort_values(by=["relative_start_time", "relative_end_time"])
        df["thread"] = 0

        parellel_processing = {k: {"end_time": None} for k in range(200)} # Setting an arbitrary max number of threads

        for idx, row in df.iterrows():
            for m, v in parellel_processing.items():
                if v["end_time"] is None or v.get("end_time") < row["relative_start_time"]:
                    parellel_processing[m] = {"end_time": row["relative_end_time"]}
                    df.loc[idx, "thread"] = m
                    break

