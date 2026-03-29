"""
pipeline/assets/silver.py
SILVER LAYER – dbt via subprocess (compatible dengan @asset decorator)

dbt.cli(..., context=context) hanya valid di @dbt_assets.
Di @asset biasa, kita panggil dbt langsung lewat subprocess.

Flow:
  ingest_csv
      ↓
  test_bronze   (dbt test --select source:bronze)
      ↓
  run_silver    (dbt run  --select silver)
      ↓
  test_silver   (dbt test --select silver)
"""
import json
import os
import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, Output, asset

from pipeline.assets.bronze import ingest_csv

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"


def _run_dbt(context: AssetExecutionContext, args: list[str], step_label: str) -> dict:
    """
    Jalankan dbt via subprocess, stream output ke Dagster log,
    raise RuntimeError jika ada node fail/error.
    """
    profiles_dir = str(DBT_PROJECT_DIR)
    project_dir  = str(DBT_PROJECT_DIR)
    target       = os.environ.get("DBT_TARGET", "dev")

    cmd = [
        "dbt", *args,
        "--profiles-dir", profiles_dir,
        "--project-dir",  project_dir,
        "--target",       target,
        "--no-version-check",
    ]
    context.log.info(f"[dbt] {step_label}: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    # Log stdout + stderr ke Dagster UI
    for line in (result.stdout + result.stderr).splitlines():
        if line.strip():
            context.log.info(line)

    # Parse run_results.json untuk summary dan deteksi failure
    run_results_path = DBT_PROJECT_DIR / "target" / "run_results.json"
    summary: dict = {"step": step_label}

    if run_results_path.exists():
        with open(run_results_path) as f:
            rr = json.load(f)
        results = rr.get("results", [])
        passed  = sum(1 for r in results if r.get("status") in ("pass", "success"))
        failed  = sum(1 for r in results if r.get("status") in ("fail", "error"))
        summary.update({"passed": passed, "failed": failed, "total": len(results)})

        if failed:
            failed_nodes = [
                r.get("unique_id", "?")
                for r in results
                if r.get("status") in ("fail", "error")
            ]
            raise RuntimeError(
                f"[dbt] {step_label} FAILED – {failed}/{len(results)} node gagal:\n"
                + "\n".join(f"  • {n}" for n in failed_nodes)
            )

    if result.returncode != 0 and not run_results_path.exists():
        raise RuntimeError(
            f"[dbt] {step_label} exited {result.returncode}:\n{result.stderr}"
        )

    context.log.info(f"[dbt] {step_label} selesai: {summary}")
    return summary


# ── Bronze test (quality gate) ────────────────────────────────────────────────
@asset(
    group_name="bronze",
    compute_kind="dbt-test",
    deps=[ingest_csv],
    description=(
        "dbt test --select source:bronze | "
        "Validasi semua tabel Bronze sebelum Silver dijalankan. "
        "Gagal di sini → run_silver tidak dieksekusi."
    ),
)
def test_bronze(context: AssetExecutionContext):
    summary = _run_dbt(context, ["test", "--select", "source:bronze"], "test_bronze")
    return Output(
        summary,
        metadata={k: v for k, v in summary.items() if isinstance(v, (int, str))},
    )


# ── Silver run ────────────────────────────────────────────────────────────────
@asset(
    group_name="silver",
    compute_kind="dbt",
    deps=[test_bronze],
    description=(
        "dbt run --select silver | "
        "Jalankan 4 model silver (incremental merge): "
        "stg_customers, stg_sales, stg_after_sales, stg_customer_addresses."
    ),
)
def run_silver(context: AssetExecutionContext):
    summary = _run_dbt(context, ["run", "--select", "silver"], "run_silver")
    return Output(
        summary,
        metadata={k: v for k, v in summary.items() if isinstance(v, (int, str))},
    )


# ── Silver test ───────────────────────────────────────────────────────────────
@asset(
    group_name="silver",
    compute_kind="dbt-test",
    deps=[run_silver],
    description=(
        "dbt test --select silver | "
        "Validasi data Silver: uniqueness, not_null, accepted_values, range checks. "
        "Gagal di sini → run_gold tidak dieksekusi."
    ),
)
def test_silver(context: AssetExecutionContext):
    summary = _run_dbt(context, ["test", "--select", "silver"], "test_silver")
    return Output(
        summary,
        metadata={k: v for k, v in summary.items() if isinstance(v, (int, str))},
    )
