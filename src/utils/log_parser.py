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
    
    def parse_logs(self):
        model_pattern = re.findall(r"(.+)\s+\d+ of \d+ OK created .*?\.(\w+) .* \[OK in (\d+\.\d+)s\]", self.log_data)
        base_day = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
        previous_end_time_hh = 0
        
        results = []
        for end_time, model_name, run_time in model_pattern:
            if len(end_time.split(":"))==3:
                end_time_hh, end_time_mm, end_time_s = end_time.split(":")
                if int(end_time_hh) < previous_end_time_hh:
                    base_day += datetime.timedelta(days=1)
                end_time = base_day + datetime.timedelta(hours=int(end_time_hh), minutes=int(end_time_mm), seconds=int(end_time_s))
                previous_end_time_hh = int(end_time_hh)
            else:
                end_time = datetime.datetime.strptime(end_time, "%y-%m-%d %H:%M:%S")
            
            start_time = end_time - datetime.timedelta(seconds=float(run_time))
            results.append({
                "start_time": start_time,
                "end_time": end_time,
                "model_name": model_name,
                "run_time": float(run_time)
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
    