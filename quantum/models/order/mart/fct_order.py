import time

def model(dbt, session):
    time.sleep(4)
    dbt.config(materialized= "table")

    df = dbt.ref("stg_order").to_df()

    return df