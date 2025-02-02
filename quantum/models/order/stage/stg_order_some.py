import pandas as pd
import time

def model(dbt, session):
    time.sleep(5)
    dbt.config(materialized= "table")
    dfs = []
    dfs.append(dbt.ref("e_order_event_1").to_df())
    dfs.append(dbt.ref("e_order_event_2").to_df())
    dfs.append(dbt.ref("e_order_event_3").to_df())
    df = pd.concat(dfs)
    return df