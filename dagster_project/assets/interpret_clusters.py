from dagster import asset, ResourceParam
import pandas as pd
import os

@asset(deps=["clustered_benchmark_data"])
def llm_cluster_analysis(openai_client: ResourceParam) -> str:
    """Sends cluster summary to LLM and stores response for later validation."""
    
    # Load clustered data
    df = pd.read_parquet("data/clustered_benchmark_data.parquet")

    # Prepare cluster summary
    def mode(series):
        return "N/A" if series.mode().empty else series.mode()[0]

    summary = (
        df.groupby("cluster")
        .agg({
            "rows": "mean",
            "columns": "mean",
            "null_rate": "mean",
            "cardinality": "mean",
            "runtime_ms": "mean",
            "engine": mode,
            "output_format": mode,
            "cluster": "count"
        })
        .rename(columns={"cluster": "n_runs"})
        .reset_index()
    )

    # Format prompt
    prompt = f"""
You are analysing the results of a performance benchmarking experiment.

Each cluster groups workloads that behave similarly. Here is a summary table (Markdown):

{summary.to_markdown(index=False)}

Columns:
- rows, columns, null_rate, cardinality: input features
- runtime_ms: average performance
- engine/output_format: most common tool
- n_runs: number of runs in this cluster

Please:
1. Describe key patterns in runtime across clusters.
2. Explain how null rate, cardinality, and output format affect performance.
3. Highlight any transitions or thresholds (e.g. "cardinality > 10000 slows down").
4. Identify efficient or inefficient configurations.
5. Write 3–5 general performance heuristics.

Keep your answer concise but structured.
    """

    # Query OpenAI LLM
    response_text = openai_client(prompt)

    # Write response to file
    output_path = "data/llm_cluster_analysis.md"
    os.makedirs("data", exist_ok=True)
    with open(output_path, "w") as f:
        f.write("# LLM Cluster Interpretation\n\n")
        f.write(response_text)

    return response_text