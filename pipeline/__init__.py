"""
pipeline/__init__.py
Dagster Definitions – AstraWorld Data Engineering Pipeline

Flow:
  generate_data → ingest_csv → test_bronze
                                    ↓
                               run_silver → test_silver
                                                ↓
                                           run_gold → test_gold
"""
from dagster import Definitions

from pipeline.assets import (
    generate_data,
    ingest_csv,
    test_bronze,
    run_silver,
    test_silver,
    run_gold,
    test_gold,
)
from pipeline.jobs    import full_pipeline_job, daily_schedule
from pipeline.sensors import csv_file_sensor

defs = Definitions(
    assets=[
        generate_data,
        ingest_csv,
        test_bronze,
        run_silver,
        test_silver,
        run_gold,
        test_gold,
    ],
    jobs=[full_pipeline_job],
    schedules=[daily_schedule],
    sensors=[csv_file_sensor],
)
