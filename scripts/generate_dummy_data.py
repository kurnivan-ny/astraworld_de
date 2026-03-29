"""
scripts/generate_dummy_data.py
Generate dummy data untuk testing pipeline AstraWorld.

Output:
  • Insert rows ke astraworld_dw (MySQL) – customers, sales, after_sales
  • Write CSV harian ke data/dummy/customer_addresses_<YYYYMMDD>.csv

Usage:
  python scripts/generate_dummy_data.py
  python scripts/generate_dummy_data.py --rows 50 --date 20260401
  python scripts/generate_dummy_data.py --csv-only
"""
import argparse
import csv
import os
import random
import re
from datetime import datetime, timedelta
from pathlib import Path

import pymysql
from faker import Faker

fake = Faker("id_ID")
random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "de_user")
MYSQL_PASS = os.getenv("MYSQL_PASS", "de_pass")
DB_RAW     = os.getenv("DB_RAW",     "astraworld_dw")

MODELS = ["RAIZA", "RANGGO", "INNAVO", "VELOS", "ALTROS", "GRAN MAX"]
BASE_PRICES = {
    "RAIZA":    200_000_000,
    "RANGGO":   350_000_000,
    "INNAVO":   530_000_000,
    "VELOS":    380_000_000,
    "ALTROS":   180_000_000,
    "GRAN MAX": 210_000_000,
}
SERVICE_TYPES = ["BP", "PM", "GR"]

# intentional dob format mess – mirrors raw layer reality
DOB_FMTS = [
    lambda d: d.strftime("%Y-%m-%d"),
    lambda d: d.strftime("%Y/%m/%d"),
    lambda d: d.strftime("%d/%m/%Y"),
    lambda d: d.strftime("%m-%d-%Y"),
    lambda _: "1900-01-01",   # sentinel
    lambda _: None,           # NULL (company)
]

DATA_DIR = Path(__file__).parent.parent / "data" / "dummy"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def dirty_price(amount: int) -> str:
    """350000000 → '350.000.000' (Indonesian thousands sep)"""
    return f"{amount:,}".replace(",", ".")


def random_vin() -> str:
    L = "ABCDEFGHJKLMNPRSTUVWXYZ"
    D = "0123456789"
    return "".join(random.choices(L, k=3)) + \
           "".join(random.choices(D, k=4)) + \
           "".join(random.choices(L, k=3))


def gen_customers(n: int, start_id: int = 7):
    rows = []
    for i in range(n):
        cid  = start_id + i
        is_corp = random.random() < 0.15
        name = fake.company() if is_corp else fake.name()
        dob_raw = fake.date_of_birth(minimum_age=17, maximum_age=70)
        dob = (None if is_corp else random.choice(DOB_FMTS)(dob_raw))
        ca  = fake.date_time_between("-1y", "now").strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        rows.append((cid, name, dob, ca))
    return rows


def gen_sales(customer_ids, n: int):
    rows, used = [], set()
    for _ in range(n):
        vin = random_vin()
        while vin in used:
            vin = random_vin()
        used.add(vin)
        cid   = random.choice(customer_ids)
        model = random.choice(MODELS)
        price = BASE_PRICES[model] + random.randint(-20_000_000, 30_000_000)
        inv_d = fake.date_between("-1y", "today")
        ca    = (datetime.combine(inv_d, datetime.min.time()) +
                 timedelta(hours=random.randint(8, 18),
                           minutes=random.randint(0, 59)))
        rows.append((
            vin, cid, model,
            inv_d.strftime("%Y-%m-%d"),
            dirty_price(price),
            ca.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        ))
    return rows


def gen_after_sales(vin_list, cust_map, n: int):
    rows, used = [], set()
    for _ in range(n):
        tk = f"T{random.randint(100,999)}-{fake.lexify('???#').lower()}"
        while tk in used:
            tk = f"T{random.randint(100,999)}-{fake.lexify('???#').lower()}"
        used.add(tk)
        vin   = random.choice(vin_list)
        cid   = cust_map.get(vin, random.choice(list(cust_map.values())))
        model = random.choice(MODELS)
        svc_d = fake.date_between("-6m", "today")
        ca    = (datetime.combine(svc_d, datetime.min.time()) +
                 timedelta(hours=random.randint(8, 17),
                           minutes=random.randint(0, 59)))
        rows.append((
            tk, vin, cid, model,
            svc_d.strftime("%Y-%m-%d"),
            random.choice(SERVICE_TYPES),
            ca.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        ))
    return rows


def gen_csv(customer_ids, target_date: str) -> Path:
    path = DATA_DIR / f"customer_addresses_{target_date}.csv"
    cities = [
        ("Bekasi",           "Jawa Barat"),
        ("Tangerang Selatan","Banten"),
        ("Jakarta Pusat",    "DKI Jakarta"),
        ("Jakarta Utara",    "DKI Jakarta"),
        ("Surabaya",         "Jawa Timur"),
        ("Bandung",          "Jawa Barat"),
        ("Semarang",         "Jawa Tengah"),
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "customer_id", "address", "city", "province", "created_at"])
        for idx, cid in enumerate(customer_ids, 1):
            city, prov = random.choice(cities)
            w.writerow([
                idx, cid, fake.street_address(), city, prov,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            ])
    print(f"[CSV]   Written → {path}")
    return path


def insert_mysql(customers, sales, after_sales):
    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        db=DB_RAW, user=MYSQL_USER, passwd=MYSQL_PASS,
        charset="utf8mb4",
    )
    cur = conn.cursor()
    if customers:
        cur.executemany(
            "INSERT IGNORE INTO customers_raw (id,name,dob,created_at) VALUES (%s,%s,%s,%s)",
            customers,
        )
    if sales:
        cur.executemany(
            "INSERT IGNORE INTO sales_raw "
            "(vin,customer_id,model,invoice_date,price,created_at) VALUES (%s,%s,%s,%s,%s,%s)",
            sales,
        )
    if after_sales:
        cur.executemany(
            "INSERT IGNORE INTO after_sales_raw "
            "(service_ticket,vin,customer_id,model,service_date,service_type,created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            after_sales,
        )
    conn.commit()
    cur.close(); conn.close()
    print(f"[MySQL] Inserted: {len(customers)} customers, "
          f"{len(sales)} sales, {len(after_sales)} after_sales → {DB_RAW}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows",     type=int, default=30)
    ap.add_argument("--date",     default=datetime.now().strftime("%Y%m%d"))
    ap.add_argument("--csv-only", action="store_true")
    args = ap.parse_args()

    customers  = gen_customers(args.rows, start_id=7)
    all_ids    = [r[0] for r in customers] + list(range(1, 7))
    sales      = gen_sales(all_ids, args.rows)
    vin_list   = [r[0] for r in sales]
    cust_map   = {r[0]: r[1] for r in sales}
    after      = gen_after_sales(vin_list, cust_map, max(1, args.rows // 2))

    gen_csv(all_ids, args.date)

    if not args.csv_only:
        insert_mysql(customers, sales, after)


if __name__ == "__main__":
    main()
