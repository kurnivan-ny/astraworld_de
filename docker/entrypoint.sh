#!/bin/bash
# entrypoint.sh
# Webserver : dbt deps (with retry) → dbt compile → dagster-webserver
# Daemon    : langsung dagster-daemon run (tidak perlu dbt deps/compile)

DBT_PROJECT="/opt/dagster/app/dbt_project"
PROFILES_DIR="$DBT_PROJECT"

echo "[entrypoint] === AstraWorld Pipeline Startup ==="
echo "[entrypoint] Command: $*"

# Daemon tidak butuh dbt deps/compile – langsung start
if echo "$*" | grep -q "dagster-daemon"; then
  echo "[entrypoint] Daemon mode – skipping dbt deps/compile"
  cd /opt/dagster/app
  exec "$@"
fi

# ── Step 1: dbt deps (retry 3x) ───────────────────────────────────────────────
echo "[entrypoint] Installing dbt packages..."
cd "$DBT_PROJECT"

DBT_DEPS_OK=false
for i in 1 2 3; do
  echo "[entrypoint] dbt deps attempt $i/3..."
  if dbt deps \
      --profiles-dir "$PROFILES_DIR" \
      --project-dir  "$DBT_PROJECT" \
      2>&1; then
    DBT_DEPS_OK=true
    break
  fi
  echo "[entrypoint] dbt deps attempt $i gagal, tunggu 5 detik..."
  sleep 5
done

if [ "$DBT_DEPS_OK" = false ]; then
  echo "[entrypoint] ERROR: dbt deps gagal setelah 3 percobaan."
  echo "[entrypoint] Pastikan internet tersedia saat build. Lanjut startup..."
fi

# ── Step 2: Start Dagster webserver ───────────────────────────────────────────
cd /opt/dagster/app
echo "[entrypoint] Starting: $*"
exec "$@"