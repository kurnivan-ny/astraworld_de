"""
pipeline/assets/bronze.py
BRONZE LAYER
  • generate_data  – jalankan scripts/generate_dummy_data.py (MySQL + CSV)
  • ingest_csv     – CSV harian → astraworld_dw.customer_addresses_raw via Sling

Fix Sling connection:
  Sling CLI membaca koneksi dari ~/.sling/env.yaml, bukan dari env var
  SLING_<NAME> langsung. Solusi: tulis env.yaml sebelum subprocess.run,
  lalu bersihkan setelahnya.
"""
import glob
import os
import subprocess
import textwrap
from datetime import datetime
from pathlib import Path

import yaml

from dagster import AssetExecutionContext, Output, asset

from pipeline.resources import CSV_DROP_DIR

# ── helper: tulis Sling env.yaml ─────────────────────────────────────────────
def _write_sling_env(host: str, port: str, user: str, pw: str, db: str) -> Path:
    """
    Tulis ~/.sling/env.yaml dengan koneksi MYSQL_RAW.
    Sling CLI membaca file ini untuk resolve --tgt-conn MYSQL_RAW.
    Returns path file yang ditulis (untuk cleanup).
    """
    sling_dir = Path.home() / ".sling"
    sling_dir.mkdir(parents=True, exist_ok=True)
    env_path = sling_dir / "env.yaml"

    config = {
        "connections": {
            "MYSQL_RAW": {
                "type":     "mysql",
                "host":     host,
                "port":     int(port),
                "user":     user,
                "password": pw,
                "database": db,
            }
        }
    }

    with open(env_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    return env_path


# ── 1. Generate Data ──────────────────────────────────────────────────────────
@asset(
    group_name="bronze",
    compute_kind="python",
    description=(
        "Generate dummy data: insert customers/sales/after_sales ke astraworld_dw (MySQL) "
        "dan buat CSV customer_addresses_<YYYYMMDD>.csv di data/dummy/."
    ),
)
def generate_data(context: AssetExecutionContext):
    rows  = int(os.environ.get("GENERATE_ROWS", "30"))
    today = datetime.now().strftime("%Y%m%d")

    script = Path(__file__).parent.parent.parent / "scripts" / "generate_dummy_data.py"
    cmd    = ["python", str(script), "--rows", str(rows), "--date", today]

    context.log.info(f"Running generate script: rows={rows}, date={today}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"generate_dummy_data.py failed:\n{result.stderr}")

    context.log.info(result.stdout.strip())
    return Output(
        {"rows": rows, "date": today},
        metadata={"rows_generated": rows, "run_date": today},
    )


# ── 2. Ingest CSV via Sling ───────────────────────────────────────────────────
@asset(
    group_name="bronze",
    compute_kind="sling",
    deps=[generate_data],
    description=(
        "Ingest CSV harian customer_addresses_<YYYYMMDD>.csv → "
        "astraworld_dw.customer_addresses_raw via Sling (mode: upsert, PK: id)."
    ),
)
def ingest_csv(context: AssetExecutionContext):
    today   = datetime.now().strftime("%Y%m%d")
    pattern = str(CSV_DROP_DIR / f"customer_addresses_{today}.csv")
    files   = sorted(glob.glob(pattern))

    if not files:
        raise RuntimeError(
            f"CSV tidak ditemukan untuk {today} di {CSV_DROP_DIR}. "
            "Pastikan generate_data sudah berjalan lebih dulu."
        )

    csv_path = files[-1]
    context.log.info(f"Ingesting: {csv_path}")

    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    user = os.environ["MYSQL_USER"]
    pw   = os.environ["MYSQL_PASS"]
    db   = os.environ.get("DB_NAME", "astraworld_dw")

    env_path = _write_sling_env(host, port, user, pw, db)
    context.log.info(f"Sling env.yaml written to {env_path}")

    try:
        cmd = [
            "sling", "run",
            "--src-conn",    "LOCAL",
            "--src-stream",  csv_path,
            "--tgt-conn",    "MYSQL_RAW",
            "--tgt-object",  f"{db}.customer_addresses_raw",
            "--mode",        "incremental",
            "--primary-key", "id",
            "--update-key",  "created_at"
        ]


        context.log.info(f"Sling cmd: {' '.join(cmd)}")
        print(cmd)
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.stdout:
            context.log.info(result.stdout.strip())
        if result.stderr:
            context.log.info(result.stderr.strip())

        if result.returncode != 0:
            raise RuntimeError(
                f"Sling exited {result.returncode}:\n{result.stderr}"
            )
    finally:
        if env_path.exists():
            env_path.unlink()
            context.log.info("Sling env.yaml cleaned up")

    return Output(
        {"file": csv_path, "date": today},
        metadata={"csv_path": csv_path, "run_date": today},
    )