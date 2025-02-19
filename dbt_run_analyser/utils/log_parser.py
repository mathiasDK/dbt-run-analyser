import re
import polars as pl
import datetime


class LogParser:
    """
    A class to parse log files and extract model run times.

    Attributes:
        log_file (str): Path to the log file.
        log_data (str): Content of the log file.
    """

    def __init__(self, log_file):
        """
        Initializes the LogParser with the given log file.

        Args:
            log_file (str): Path to the log file.
        """
        self.log_file = log_file
        self.log_data = self._read_log()

    def _read_log(self):
        """
        Reads the log file and returns its content.

        Returns:
            str: Content of the log file.
        """
        with open(self.log_file, "r") as file:
            return file.read()

    def _parse_timestamp(self, s: str):
        """
        Parses a timestamp from a log line.

        Args:
            s (str): A line from the log file.

        Returns:
            datetime.datetime: Parsed timestamp.
        """
        pattern = r"(\d{2}:\d{2}:\d{2})"
        t = re.findall(pattern, s)
        if isinstance(t, list):  # if there are multiple timestamps
            t = t[-1]
        t = "2025-01-01 " + t
        t = datetime.datetime.strptime(
            t, "%Y-%m-%d %H:%M:%S"
        )  # converting to datetime object
        return t

    def _parse_model_name(self, s: str):
        """
        Parses a model name from a log line.

        Args:
            s (str): A line from the log file.

        Returns:
            str or None: Parsed model name or None if not found.
        """
        pattern = r"\s\w+\.(\w+)\s"
        model_name = re.findall(pattern, s)
        if len(model_name) == 0:
            return None

        return model_name[0]

    def parse_logs(self):
        """
        Parses the log data to extract model run times and returns a DataFrame.

        Returns:
            pl.DataFrame: DataFrame containing model run times with relative start and end times.
        """
        start_times = {}

        results = []
        for s in self.log_data.split("\n"):
            if " START " in s:
                start_time = self._parse_timestamp(s)
                model_name = self._parse_model_name(s)
                if model_name is not None:
                    start_times[model_name] = start_time
            if "OK created" in s:
                end_time = self._parse_timestamp(s)
                model_name = self._parse_model_name(s)
                if model_name is None:
                    continue

                start_time = start_times.get(model_name)
                try:
                    if (
                        end_time < start_time
                    ):  # If the end time is less than the start time, it means the model ran into the next day
                        end_time += datetime.timedelta(days=1)
                except TypeError:
                    print(f"Model {model_name} does not have a start time")
                    continue
                end_time += datetime.timedelta(
                    milliseconds=-10
                )  # Doing this to avoid overlapping of time
                run_time = (end_time - start_time).total_seconds()

                results.append(
                    {
                        "start_time": start_time,
                        "end_time": end_time,
                        "model_name": model_name,
                        "run_time": run_time,
                    }
                )

        df = pl.DataFrame(results)
        first_start_time = df["start_time"].min()
        df = df.with_columns(
            # Converting timedelta to ns and then to s by dividing by 1e6
            ((pl.col("start_time") - first_start_time) / 1e6)
            .cast(pl.Int64)
            .alias("relative_start_time"),
            ((pl.col("end_time") - first_start_time).cast(pl.Float64) / 1e6).alias(
                "relative_end_time"
            ),
        ).drop(["start_time", "end_time"])

        return df
