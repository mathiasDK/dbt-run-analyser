import pandas as pd
import time


def model(dbt, session):
    time.sleep(12)
    dbt.config(materialized="table")
    dfs = []
    dfs.append(dbt.ref("dim_customer").to_df())
    dfs.append(dbt.ref("dim_store").to_df())
    dfs.append(dbt.ref("fct_order").to_df())
    df = pd.concat(dfs)
    return df
