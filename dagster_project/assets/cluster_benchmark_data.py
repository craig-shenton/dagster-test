from dagster import asset
import pandas as pd
from sklearn.preprocessing import StandardScaler
import hdbscan
import os

@asset(deps=["synthetic_benchmark_data"])
def clustered_benchmark_data() -> pd.DataFrame:
    df = pd.read_parquet("data/benchmark_results.parquet")

    # Select features for clustering
    features = df[["rows", "columns", "null_rate", "cardinality", "runtime_ms"]]
    scaled = StandardScaler().fit_transform(features)

    # Run HDBSCAN
    clusterer = hdbscan.HDBSCAN(min_cluster_size=25)
    df["cluster"] = clusterer.fit_predict(scaled)

    # Save clustered output
    output_path = "data/clustered_benchmark_data.parquet"
    df.to_parquet(output_path, index=False)

    return df