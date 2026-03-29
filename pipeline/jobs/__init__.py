"""
pipeline/jobs/__init__.py
Dagster jobs + schedule

full_pipeline_job  – end-to-end: generate → ingest → test bronze
                     → silver → test silver → gold → test gold
"""
from dagster import AssetSelection, define_asset_job, ScheduleDefinition

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    # Pilih semua asset dari group bronze, silver, gold
    # (termasuk test_bronze, test_silver, test_gold)
    selection=AssetSelection.groups("bronze", "silver", "gold"),
    description=(
        "End-to-end pipeline: generate data → ingest CSV (Sling) → "
        "dbt test bronze → dbt run silver → dbt test silver → "
        "dbt run gold → dbt test gold. "
        "Runs daily 01:00 WIB (18:00 UTC)."
    ),
)

daily_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 18 * * *",   # 01:00 WIB = 18:00 UTC
    name="daily_pipeline_schedule",
)
