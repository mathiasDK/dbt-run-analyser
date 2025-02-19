import pandas as pd
import time


def model(dbt, session):
    time.sleep(10)
    dbt.config(materialized="table")

    dfs = []
    dfs.append(dbt.ref("e_order_event_1").to_df())
    dfs.append(dbt.ref("e_order_event_2").to_df())
    dfs.append(dbt.ref("e_order_event_3").to_df())
    dfs.append(dbt.ref("e_order_event_4").to_df())
    dfs.append(dbt.ref("e_order_event_5").to_df())
    dfs.append(dbt.ref("e_order_event_6").to_df())
    dfs.append(dbt.ref("e_order_event_7").to_df())

    df = pd.concat(dfs)
    return df
