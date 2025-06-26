from dagster import resource
import os

@resource(config_schema={"output_dir": str})
def csv_writer_resource(context):
    """Creates a resource for writing DataFrames to CSV files in a specified output directory.

    This resource ensures the output directory exists and provides a function to write DataFrames as CSV files.

    Args:
        context: Dagster resource context containing configuration.

    Returns:
        Callable: A function that writes a DataFrame to a CSV file in the output directory.
    """
    output_dir = context.resource_config["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    def write_csv(df, filename):
        path = os.path.join(output_dir, filename)
        df.to_csv(path, index=False)
        context.log.info(f"Wrote {path}")

    return write_csv

@resource(config_schema={"output_dir": str})
def parquet_writer_resource(context):
    """Creates a resource for writing DataFrames to Parquet files in a specified output directory.
    
    This resource ensures the output directory exists and provides a function to write DataFrames as Parquet files.

    Args:
        context: Dagster resource context containing configuration.

    Returns:
        Callable: A function that writes a DataFrame to a Parquet file in the output directory.
    """
    output_dir = context.resource_config["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    def write_parquet(df, filename):
        path = os.path.join(output_dir, filename)
        df.to_parquet(path, index=False)
        context.log.info(f"Wrote {path}")

    return write_parquet