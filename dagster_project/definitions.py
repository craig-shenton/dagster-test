import os
from dagster import Definitions
from dagster_project.assets import interpret_clusters, synthetic_data, cluster_benchmark_data
from dagster_project.resources.io_local import csv_writer_resource, parquet_writer_resource
from dagster_project.resources.llm_openai import openai_client

defs = Definitions(
    assets=[
        synthetic_data.synthetic_benchmark_data,
        cluster_benchmark_data.clustered_benchmark_data,
        interpret_clusters.llm_cluster_analysis,
    ],
    resources={
        "csv_writer": csv_writer_resource.configured({"output_dir": "data"}),
        "parquet_writer": parquet_writer_resource.configured({"output_dir": "data"}),
        "openai_client": openai_client.configured({
            "api_key": os.environ["OPENAI_API_KEY"],
            "model": "gpt-3.5-turbo"  # or "gpt-4" if needed
        }),
    }
)