from dagster import asset
import pandas as pd

@asset
def employees() -> pd.DataFrame:
    return pd.read_csv("data/employees.csv")

@asset
def departments() -> pd.DataFrame:
    return pd.read_csv("data/departments.csv")