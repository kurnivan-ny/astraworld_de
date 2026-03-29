"""
pipeline/assets/gold.py
GOLD LAYER – dbt via subprocess (compatible dengan @asset decorator)

Flow (lanjutan dari silver.py):
  test_silver
      ↓
  run_gold    (dbt run  --select gold)
      ↓
  test_gold   (dbt test --select gold)
"""
import json
import os
import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, Output, asset

from pipeline.assets.silver import test_silver

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"


def _run_dbt(context: AssetExecutionContext, args: list[str], step_label: str) -> dict:
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

    for line in (result.stdout + result.stderr).splitlines():
        if line.strip():
            context.log.info(line)

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


# ── Gold run ──────────────────────────────────────────────────────────────────
@asset(
    group_name="gold",
    compute_kind="dbt",
    deps=[test_silver],
    description=(
        "dbt run --select gold | "
        "Jalankan dm_sales_by_model & dm_customer_service (incremental merge)."
    ),
)
def run_gold(context: AssetExecutionContext):
    summary = _run_dbt(context, ["run", "--select", "gold"], "run_gold")
    return Output(
        summary,
        metadata={k: v for k, v in summary.items() if isinstance(v, (int, str))},
    )


# ── Gold test ─────────────────────────────────────────────────────────────────
@asset(
    group_name="gold",
    compute_kind="dbt-test",
    deps=[run_gold],
    description=(
        "dbt test --select gold | "
        "Validasi akhir Gold: periode format, class/priority, range checks."
    ),
)
def test_gold(context: AssetExecutionContext):
    summary = _run_dbt(context, ["test", "--select", "gold"], "test_gold")
    return Output(
        summary,
        metadata={k: v for k, v in summary.items() if isinstance(v, (int, str))},
    )
