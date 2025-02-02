import time

def model(dbt, session):
    time.sleep(2)
    dbt.config(materialized= "table")

    df = dbt.ref("stg_order").to_df()

    return df