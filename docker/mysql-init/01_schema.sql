-- ============================================================
-- AstraWorld – single DB architecture
-- ============================================================

-- 1. astraworld_dw – semua layer (bronze raw, silver stg, gold dm)
CREATE DATABASE IF NOT EXISTS astraworld_dw CHARACTER SET utf8mb4;

-- 2. metadata_db – Dagster run storage
CREATE DATABASE IF NOT EXISTS metadata_db CHARACTER SET utf8mb4;

-- Grant de_user
GRANT ALL PRIVILEGES ON astraworld_dw.* TO 'de_user'@'%';
GRANT ALL PRIVILEGES ON metadata_db.*   TO 'de_user'@'%';
FLUSH PRIVILEGES;

-- ── Bronze raw tables ─────────────────────────────────────────────────────────
USE astraworld_dw;

-- customers_raw: DOB sengaja VARCHAR (format campur)
CREATE TABLE IF NOT EXISTS customers_raw (
    id          INT          NOT NULL,
    name        VARCHAR(255) NOT NULL,
    dob         VARCHAR(50)  DEFAULT NULL,
    created_at  VARCHAR(30)  NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- sales_raw: price sengaja VARCHAR (titik ribuan Indonesia)
CREATE TABLE IF NOT EXISTS sales_raw (
    vin          VARCHAR(20)  NOT NULL,
    customer_id  INT          NOT NULL,
    model        VARCHAR(100) NOT NULL,
    invoice_date VARCHAR(30)  NOT NULL,
    price        VARCHAR(50)  NOT NULL,
    created_at   VARCHAR(30)  NOT NULL,
    PRIMARY KEY (vin)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- after_sales_raw
CREATE TABLE IF NOT EXISTS after_sales_raw (
    service_ticket  VARCHAR(30)  NOT NULL,
    vin             VARCHAR(20)  NOT NULL,
    customer_id     INT          NOT NULL,
    model           VARCHAR(100) NOT NULL,
    service_date    VARCHAR(30)  NOT NULL,
    service_type    VARCHAR(10)  NOT NULL,
    created_at      VARCHAR(30)  NOT NULL,
    PRIMARY KEY (service_ticket)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- customer_addresses_raw: diisi harian via Sling CSV ingest
CREATE TABLE IF NOT EXISTS customer_addresses_raw (
    id          INT          NOT NULL,
    customer_id INT          NOT NULL,
    address     VARCHAR(500) NOT NULL,
    city        VARCHAR(100) NOT NULL,
    province    VARCHAR(100) NOT NULL,
    created_at  VARCHAR(30)  NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Seed data ─────────────────────────────────────────────────────────────────
INSERT IGNORE INTO customers_raw VALUES
  (1, 'Antonio',       '1998-08-04',  '2025-03-01 14:24:40.012'),
  (2, 'Brandon',       '2001-04-21',  '2025-03-02 08:12:54.003'),
  (3, 'Charlie',       '1980/11/15',  '2025-03-02 11:20:02.391'),
  (4, 'Dominikus',     '14/01/1995',  '2025-03-03 09:50:41.852'),
  (5, 'Erik',          '1900-01-01',  '2025-03-03 17:22:03.198'),
  (6, 'PT Black Bird', NULL,          '2025-03-04 12:52:16.122');

INSERT IGNORE INTO sales_raw VALUES
  ('JIS8135SAD', 1, 'RAIZA',  '2025-03-01', '350.000.000', '2025-03-01 14:24:40.012'),
  ('MAS8160POE', 3, 'RANGGO', '2025-05-19', '430.000.000', '2025-05-19 14:29:21.003'),
  ('JLK1368KDE', 4, 'INNAVO', '2025-05-22', '600.000.000', '2025-05-22 16:10:28.120'),
  ('JLK1869KDF', 6, 'VELOS',  '2025-08-02', '390.000.000', '2025-08-02 14:04:31.021'),
  ('JLK1962KOP', 6, 'VELOS',  '2025-08-02', '390.000.000', '2025-08-02 15:21:04.201');

INSERT IGNORE INTO after_sales_raw VALUES
  ('T124-kgu1', 'MAS8160POE', 3, 'RANGGO', '2025-07-11', 'BP', '2025-07-11 09:24:40.012'),
  ('T560-jga1', 'JLK1368KDE', 4, 'INNAVO', '2025-08-04', 'PM', '2025-08-04 10:12:54.003'),
  ('T521-oai8', 'POI1059IIK', 5, 'RAIZA',  '2026-09-10', 'GR', '2026-09-10 12:45:02.391');

INSERT IGNORE INTO customer_addresses_raw VALUES
  (1, 1, 'Jalan Mawar V, RT 1/RW 2',         'Bekasi',            'Jawa Barat',  '2026-03-01 14:24:40.012'),
  (2, 3, 'Jl Ababil Indah',                   'Tangerang Selatan', 'Banten',      '2026-03-01 14:24:40.012'),
  (3, 4, 'Jl. Kemang Raya 1 No 3',            'Jakarta Pusat',     'DKI Jakarta', '2026-03-01 14:24:40.012'),
  (4, 6, 'Astra Tower, Jalan Yos Sudarso 12', 'Jakarta Utara',     'DKI Jakarta', '2026-03-01 14:24:40.012');
