import re
import polars as pl
import datetime

class LogParser:
    def __init__(self, log_file):
        self.log_file = log_file
        self.log_data = self._read_log()
    
    def _read_log(self):
        with open(self.log_file, 'r') as file:
            return file.read()
        
    def _parse_timestamp(self, s:str):
        pattern = r'(\d{2}:\d{2}:\d{2})'
        t = re.findall(pattern, s)
        if isinstance(t, list): # if there are multiple timestampls
            t = t[0]
        t = "2025-01-01 " + t
        t = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S") # converting to datetime object
        return t
    
    def _parse_model_name(self, s:str):
        pattern = r'\w*\s\.'
        model_name = re.findall(pattern, s)[0][:-2]

        # Find all matches
        return model_name
    
    def parse_logs(self):
        start_times = {}
        
        results = []
        for s in self.log_data.split("\n"):
            if "START" in s:
                start_time = self._parse_timestamp(s)
                model_name = self._parse_model_name(s)
                start_times[model_name] = start_time
            if "OK created" in s:
                end_time = self._parse_timestamp(s)
                model_name = self._parse_model_name(s)

                start_time = start_times.get(model_name)
                if end_time < start_time: # If the end time is less than the start time, it means the model ran into the next day
                    end_time += datetime.timedelta(days=1)
                end_time += datetime.timedelta(milliseconds=-10) # Doing this to avoid overlapping of time
                run_time = (end_time - start_time).total_seconds()
                
                results.append({
                    "start_time": start_time,
                    "end_time": end_time,
                    "model_name": model_name,
                    "run_time": run_time
                })

        df = pl.DataFrame(results)
        first_start_time = df["start_time"].min()
        df = df.with_columns(
            # Converting timedelta to ns and then to s by dividing by 1e6
            ((pl.col("start_time") - first_start_time)/1e6).cast(pl.Int64).alias("relative_start_time"),
            ((pl.col("end_time") - first_start_time).cast(pl.Float64)/1e6).alias("relative_end_time"),
        ).drop(["start_time", "end_time"])
        
        return df

if __name__ == "__main__":
    path = "test_data/cli_output/dbt_1_thread.log"
    l = LogParser(path)
    d = l.parse_logs()
    print(d.head(10))
    print(d.tail(5))
    