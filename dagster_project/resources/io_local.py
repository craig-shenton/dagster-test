from dagster import resource
import os

@resource(config_schema={"output_dir": str})
def csv_writer_resource(context):
    output_dir = context.resource_config["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    def write_csv(df, filename):
        path = os.path.join(output_dir, filename)
        df.to_csv(path, index=False)
        context.log.info(f"Wrote {path}")

    return write_csv