import pandas as pd
import time


def model(dbt, session):
    time.sleep(6)
    dbt.config(materialized="table")

    df = pd.DataFrame(data={"id": [1]})

    return df
