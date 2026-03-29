"""
pipeline/sensors/__init__.py
File-based sensor: trigger full_pipeline_job saat ada CSV baru di data/dummy/.
Sensor aktif setiap 60 detik.
"""
import glob
from pathlib import Path

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from pipeline.resources import CSV_DROP_DIR

SEEN_FILE = Path("/tmp/.astraworld_seen_csvs")


@sensor(job_name="full_pipeline_job", minimum_interval_seconds=60)
def csv_file_sensor(context: SensorEvaluationContext):
    """
    Memantau folder data/dummy/ untuk file customer_addresses_*.csv baru.
    Setiap file baru memicu satu RunRequest (deduplikasi via run_key).
    """
    seen = (
        set(SEEN_FILE.read_text().splitlines())
        if SEEN_FILE.exists()
        else set()
    )
    new = set(glob.glob(str(CSV_DROP_DIR / "customer_addresses_*.csv"))) - seen

    if not new:
        yield SkipReason("Tidak ada file CSV baru di data/dummy/.")
        return

    for f in sorted(new):
        seen.add(f)
        context.log.info(f"[sensor] CSV baru terdeteksi: {f}")
        yield RunRequest(run_key=f, run_config={})

    SEEN_FILE.write_text("\n".join(seen))
