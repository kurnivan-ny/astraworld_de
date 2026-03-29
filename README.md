# AstraWorld Data Engineering Pipeline

End-to-end **Medallion Architecture** (Bronze → Silver → Gold) berbasis MySQL,
diorkestrasikan oleh **Dagster**, transformasi & testing via **dbt + dbt_expectations**,
dan data landing harian via **Sling**.

---

## Stack

| Layer | Tools | Database |
|-------|-------|----------|
| Bronze (landing) | Python Faker + Sling CLI | `astraworld_dw` |
| Silver (cleaning) | dbt incremental (merge) | `astraworld_dw` |
| Gold (datamart) | dbt incremental (merge) | `astraworld_dw` |
| Orchestration | Dagster 1.9 | — |
| Data Quality | dbt tests + dbt_expectations | per-layer |
| Metadata/state | Dagster MySQL storage | `metadata_db` |

> Semua layer (Bronze, Silver, Gold) berada dalam satu database `astraworld_dw`.
> Isolasi antar layer menggunakan prefix nama tabel (`*_raw`, `stg_*`, `dm_*`).
> Ini diperlukan karena dbt MySQL adapter tidak mendukung cross-database `ref()`.

---

## Alur Pipeline (DAG)

```
generate_data           ← scripts/generate_dummy_data.py
    │                     (insert MySQL: customers/sales/after_sales)
    │                     (write CSV: data/dummy/customer_addresses_YYYYMMDD.csv)
    ▼
ingest_csv              ← Sling upsert CSV → astraworld_dw.customer_addresses_raw
    │
    ▼
test_bronze             ← dbt test --select source:bronze  (via subprocess)
    │                     (validasi tabel raw: not_null, unique, accepted_values)
    │                     ✗ FAIL → pipeline berhenti, Silver tidak jalan
    ▼
run_silver              ← dbt run --select silver  (incremental + merge)
    │                     stg_customers          → astraworld_dw
    │                     stg_sales              → astraworld_dw
    │                     stg_after_sales        → astraworld_dw
    │                     stg_customer_addresses → astraworld_dw
    ▼
test_silver             ← dbt test --select silver  (+ dbt_expectations)
    │                     ✗ FAIL → pipeline berhenti, Gold tidak jalan
    ▼
run_gold                ← dbt run --select gold  (incremental + merge)
    │                     dm_sales_by_model    → astraworld_dw
    │                     dm_customer_service  → astraworld_dw
    ▼
test_gold               ← dbt test --select gold  (+ dbt_expectations)
```

> dbt dipanggil via `subprocess.run` langsung (bukan `DbtCliResource.cli()`),
> karena `dbt.cli(context=context)` hanya kompatibel dengan `@dbt_assets` decorator,
> sedangkan semua asset di pipeline ini menggunakan `@asset` biasa.

---

## Incremental Strategy

| Model | unique_key | Strategy | Keterangan |
|-------|-----------|----------|------------|
| `stg_customers` | `id` | merge | Update data customer jika ada perubahan |
| `stg_sales` | `vin` | merge | Dedup VIN, simpan record paling baru |
| `stg_after_sales` | `service_ticket` | merge | Tambah tiket baru, flag orphan VIN |
| `stg_customer_addresses` | `customer_id` | merge | Satu baris per customer, simpan terbaru |
| `dm_sales_by_model` | `[periode, class, model]` | merge | Re-agregasi bulan yang punya data baru |
| `dm_customer_service` | `[periode, customer_id]` | merge | Re-agregasi tahun yang punya servis baru |

---

## Struktur Direktori

```
astraworld_de/
├── docker-compose.yml
├── .env
├── requirements.txt
├── workspace.yaml
│
├── docker/
│   ├── Dockerfile.dagster        # pip install + entrypoint
│   ├── entrypoint.sh             # dbt deps (retry 3x) → start dagster
│   └── mysql-init/
│       └── 01_schema.sql         # 2 DB + raw tables + seed data
│
├── pipeline/
│   ├── __init__.py               # Dagster Definitions (tanpa dbt resource)
│   ├── resources/__init__.py     # MySQL engine + path constants
│   ├── assets/
│   │   ├── bronze.py             # generate_data, ingest_csv
│   │   ├── silver.py             # test_bronze, run_silver, test_silver
│   │   └── gold.py               # run_gold, test_gold
│   ├── jobs/__init__.py          # full_pipeline_job + daily_schedule
│   └── sensors/__init__.py       # csv_file_sensor (60s polling)
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml              # single target: dev → astraworld_dw
│   ├── packages.yml              # calogica/dbt_expectations
│   └── models/
│       ├── sources.yml           # source: bronze (astraworld_dw)
│       ├── silver/
│       │   ├── stg_customers.sql
│       │   ├── stg_sales.sql
│       │   ├── stg_after_sales.sql
│       │   ├── stg_customer_addresses.sql
│       │   └── schema.yml        # dbt tests + dbt_expectations per kolom
│       └── gold/
│           ├── dm_sales_by_model.sql
│           ├── dm_customer_service.sql
│           └── schema.yml        # dbt tests + dbt_expectations per kolom
│
├── scripts/
│   └── generate_dummy_data.py    # Generate MySQL rows + CSV
│
├── sling_config/
│   └── customer_addresses_ingest.yaml
│
└── data/dummy/                   # CSV drop folder (watched by sensor)
```

---

## Database Layout

| Database | Kegunaan | Tabel |
|----------|----------|-------|
| `astraworld_dw` | Bronze + Silver + Gold | `customers_raw`, `sales_raw`, `after_sales_raw`, `customer_addresses_raw`, `stg_customers`, `stg_sales`, `stg_after_sales`, `stg_customer_addresses`, `dm_sales_by_model`, `dm_customer_service` |
| `metadata_db` | Infra | Dagster run history, schedules, event log |

---

## Cara Menjalankan

### 1. Persiapan env

```bash
# Edit .env jika perlu (default sudah sesuai docker-compose)
cat .env
```

### 2. Build & start semua service

```bash
docker-compose up --build -d
```

MySQL butuh ~15 detik untuk ready. Dagster UI: **http://localhost:3000**

Startup sequence otomatis (via `entrypoint.sh`):
1. `dbt deps` – install dbt_expectations (retry 3x jika gagal)
2. Start dagster-webserver

> `dagster-daemon` langsung start tanpa step dbt — entrypoint mendeteksi
> string `dagster-daemon` pada command dan skip step dbt secara otomatis.

### 3. Jalankan pipeline manual

Di Dagster UI → pilih **full_pipeline_job** → **Materialize all**

Atau via CLI:
```bash
docker exec astraworld_webserver \
  dagster job execute -m pipeline -j full_pipeline_job
```

### 4. Jalankan hanya Silver + Gold (re-run tanpa generate ulang)

```bash
# Di Dagster UI: pilih asset run_silver → downstream

# Atau via dbt langsung di dalam container:
docker exec astraworld_webserver bash -c \
  "cd /opt/dagster/app/dbt_project && \
   dbt run --profiles-dir . --target dev --select silver gold"
```

### 5. Run dbt tests manual per layer

```bash
# Bronze sources
docker exec astraworld_webserver bash -c \
  "cd /opt/dagster/app/dbt_project && \
   dbt test --profiles-dir . --target dev --select source:bronze"

# Silver
docker exec astraworld_webserver bash -c \
  "cd /opt/dagster/app/dbt_project && \
   dbt test --profiles-dir . --target dev --select silver"

# Gold
docker exec astraworld_webserver bash -c \
  "cd /opt/dagster/app/dbt_project && \
   dbt test --profiles-dir . --target dev --select gold"
```

### 6. Install ulang dbt packages secara manual

```bash
docker exec astraworld_webserver bash -c \
  "cd /opt/dagster/app/dbt_project && dbt deps --profiles-dir ."
```

---

## dbt Tests per Layer

### Bronze (sources.yml)
| Tabel | Test |
|-------|------|
| `customers_raw` | not_null(id), unique(id), not_null(name) |
| `sales_raw` | not_null(vin), not_null(price), not_null(customer_id) |
| `after_sales_raw` | not_null(service_ticket), unique(service_ticket), accepted_values(service_type: BP/PM/GR) |
| `customer_addresses_raw` | not_null(id), unique(id), not_null(customer_id), not_null(city) |

### Silver (silver/schema.yml)
| Model | Test |
|-------|------|
| `stg_customers` | not_null+unique(id), not_null(name), accepted_values(is_company: 0/1), expect_column_values_to_be_between(dob: 1900-01-02..2025-12-31, warn) |
| `stg_sales` | not_null+unique(vin), not_null(invoice_date), accepted_values(model), expect_column_values_to_be_between(price: 1..5B) |
| `stg_after_sales` | not_null+unique(service_ticket), not_null(service_date), accepted_values(service_type), accepted_values(is_orphan_vin: 0/1) |
| `stg_customer_addresses` | not_null+unique(customer_id), not_null(city/province/address), expect_column_value_lengths_to_be_between(city: 2..100) |

### Gold (gold/schema.yml)
| Model | Test |
|-------|------|
| `dm_sales_by_model` | not_null(periode), expect_column_values_to_match_regex(periode: YYYY-MM), accepted_values(class: LOW/MEDIUM/HIGH), expect_column_values_to_be_between(unit_terjual/total: ≥1) |
| `dm_customer_service` | not_null(customer_id/periode/customer_name), accepted_values(priority: HIGH/MED/LOW), expect_column_values_to_be_between(count_service: ≥1, periode: 2000..2100) |

---

## Schedule

Pipeline berjalan otomatis setiap hari pukul **01:00 WIB** (`0 18 * * *` UTC).

File sensor aktif setiap **60 detik** memantau `data/dummy/`.
