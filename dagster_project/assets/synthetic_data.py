from dagster import asset, ResourceParam
import pandas as pd
import numpy as np

@asset
def synthetic_benchmark_data(parquet_writer: ResourceParam) -> pd.DataFrame:
    """Generates structured synthetic benchmark data with realistic regimes."""
    np.random.seed(42)
    n_samples = 1000

    regimes = [
        {
            "name": "null_heavy_json",
            "size": int(n_samples * 0.25),
            "rows": (100_000, 500_000),
            "columns": (10, 50),
            "null_rate": (0.6, 0.9),
            "cardinality": (100, 1000),
            "engine": "pandas",
            "output_format": "json",
            "runtime_base": 1000,
        },
        {
            "name": "small_dense_parquet",
            "size": int(n_samples * 0.25),
            "rows": (1000, 5000),
            "columns": (5, 20),
            "null_rate": (0.0, 0.1),
            "cardinality": (10, 100),
            "engine": "polars",
            "output_format": "parquet",
            "runtime_base": 100,
        },
        {
            "name": "high_card_pandas",
            "size": int(n_samples * 0.25),
            "rows": (100_000, 1_000_000),
            "columns": (20, 50),
            "null_rate": (0.1, 0.3),
            "cardinality": (5000, 10000),
            "engine": "pandas",
            "output_format": "csv",
            "runtime_base": 800,
        },
        {
            "name": "wide_polars",
            "size": n_samples - 3 * int(n_samples * 0.25),
            "rows": (10_000, 100_000),
            "columns": (80, 100),
            "null_rate": (0.2, 0.4),
            "cardinality": (1000, 5000),
            "engine": "polars",
            "output_format": "parquet",
            "runtime_base": 300,
        }
    ]

    dfs = []
    for regime in regimes:
        size = regime["size"]
        df = pd.DataFrame({
            "rows": np.random.randint(*regime["rows"], size),
            "columns": np.random.randint(*regime["columns"], size),
            "null_rate": np.round(np.random.uniform(*regime["null_rate"], size), 2),
            "cardinality": np.random.randint(*regime["cardinality"], size),
            "engine": regime["engine"],
            "output_format": regime["output_format"],
        })
        # Simulate runtime with deterministic base + variability
        df["runtime_ms"] = (
            regime["runtime_base"]
            + 0.00005 * df["rows"]
            + 0.3 * df["columns"]
            + 30 * df["null_rate"]
            + 0.005 * df["cardinality"]
            + np.random.normal(0, 20, size)
        ).round(2)
        df["regime"] = regime["name"]
        dfs.append(df)

    df_all = pd.concat(dfs).reset_index(drop=True)

    parquet_writer(df_all, "benchmark_results.parquet")
    return df_all