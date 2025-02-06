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
        if isinstance(t, list):
            return t[0]
        return t
    
    def _parse_model_name(self, s:str):
        pattern = r'\w*\s\.'
        model_name = re.findall(pattern, s)[0][:-2]

        # Find all matches
        return model_name
    
    def _parse_run_time(self, s:str):
        pattern = r'in (\d+\.\d+)s'

        # Find all matches
        return float(re.findall(pattern, s)[0])
    
    def parse_logs(self):
        base_day = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
        previous_end_time_hh = 0
        
        results = []
        for s in self.log_data.split("\n"):
            if "OK created" in s:
                timestamp = self._parse_timestamp(s)
                model_name = self._parse_model_name(s)
                run_time = self._parse_run_time(s)
                end_time_hh, end_time_mm, end_time_s = timestamp.split(":")
                if int(end_time_hh) < previous_end_time_hh:
                    base_day += datetime.timedelta(days=1)
                end_time = base_day + datetime.timedelta(hours=int(end_time_hh), minutes=int(end_time_mm), seconds=int(end_time_s))
                previous_end_time_hh = int(end_time_hh)
                
                start_time = end_time - datetime.timedelta(seconds=run_time)
                results.append({
                    "start_time": start_time,
                    "end_time": end_time,
                    "model_name": model_name,
                    "run_time": run_time
                })

        df = pl.DataFrame(results)
        first_start_time = df["start_time"].min()
        df = df.with_columns(
            (pl.col("start_time") - first_start_time).alias("relative_start_time"),
            (pl.col("end_time") - first_start_time).alias("relative_end_time"),
        ).drop(["start_time", "end_time"])
        
        return df

if __name__ == "__main__":
    path = "test_data/cli_output/dbt_1_thread.log"
    l = LogParser(path)
    d = l.parse_logs()
    print(d.head(10))
    print(d.tail(5))
    