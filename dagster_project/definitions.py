from dagster import Definitions
from dagster_project.assets import raw_assets, processed_assets
from dagster_project.resources.io_local import csv_writer_resource

defs = Definitions(
    assets=[
        raw_assets.employees,
        raw_assets.departments,
        processed_assets.avg_salary_by_department,
    ],
    resources={
        "csv_writer": csv_writer_resource.configured({"output_dir": "data"})
    }
)
