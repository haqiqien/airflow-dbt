# Procurement Data Warehouse

Pipeline data engineering end-to-end untuk domain **Procurement**, dibangun di atas **Apache Airflow**, **dbt**, dan **PostgreSQL** dalam lingkungan devcontainer yang fully reproducible.

---

## Daftar Isi

1. [Arsitektur](#arsitektur)
2. [Tech Stack](#tech-stack)
3. [Struktur Direktori](#struktur-direktori)
4. [Quick Start](#quick-start)
5. [Menjalankan Pipeline](#menjalankan-pipeline)
6. [Data Model](#data-model)
7. [Lapisan dbt](#lapisan-dbt)
8. [Verifikasi Data](#verifikasi-data)
9. [Melihat Lineage Graph](#melihat-lineage-graph)
10. [Konfigurasi](#konfigurasi)
11. [Known Issues](#known-issues)

---

## Arsitektur

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       Apache Airflow DAG                                        │
│                    (procurement_pipeline)                                       │
│                                                                                 │
│  [1] generate_data  →  [2] ingest_to_postgres  →  [3] dbt_run  →  [4] dbt_test  │
│   PythonOperator        PythonOperator            BashOperator    BashOperator  │
└─────────────────────────────────────────────────────────────────────────────────┘
         │                      │                      │
         ▼                      ▼                      ▼
  data/*.csv (7 file)    raw.* (7 tabel)      staging → intermediate → marts
  (data sintetis         (PostgreSQL,          (dbt transforms,
   Faker id_ID)           full load)            Star Schema)
```

### Alur Data

| # | Stage | Tool | Output |
|---|-------|------|--------|
| 1 | **Generate** | Python + Faker | 7 file CSV di `data/` |
| 2 | **Ingest** | psycopg2 | Skema `raw` di PostgreSQL |
| 3 | **Transform** | dbt | Skema `staging`, `intermediate`, `marts` |
| 4 | **Test** | dbt test | 127 data quality tests |

---

## Tech Stack

| Komponen | Versi |
|---|---|
| Python | 3.10 |
| Apache Airflow | 2.8.1 (LocalExecutor) |
| dbt-core | 1.7.0 |
| dbt-postgres | 1.7.0 |
| PostgreSQL | 15 (Alpine) |
| Faker | latest |
| pandas | latest |
| psycopg2-binary | latest |

---

## Struktur Direktori

```
airflow-dbt/
│
├── .devcontainer/               # Konfigurasi devcontainer (VS Code + Docker)
│   ├── devcontainer.json        #   Env vars Airflow, VS Code extensions, port forwarding
│   ├── docker-compose.yml       #   Service: devcontainer + postgres:15
│   └── Dockerfile               #   Python 3.10 + semua package Python
│
├── airflow/
│   └── dags/
│       └── procurement_pipeline.py   # DAG utama (4 tasks)
│
├── data/                        # [IGNORED] CSV hasil generate_data.py
│   ├── departments.csv
│   ├── vendors.csv
│   ├── items.csv
│   ├── purchase_requests.csv
│   ├── purchase_orders.csv
│   ├── purchase_order_lines.csv
│   └── goods_receipts.csv
│
├── data_generator/
│   └── generate_data.py         # Generator data sintetis (Faker id_ID)
│
├── procurement_dw/              # Project dbt
│   ├── dbt_project.yml          #   Konfigurasi project & schema per layer
│   ├── profiles.yml             #   Koneksi PostgreSQL (target: dev)
│   ├── macros/
│   │   └── generate_schema_name.sql  # Override: cegah prefix ganda (staging_staging)
│   └── models/
│       ├── staging/             #   View langsung dari raw.*
│       │   ├── sources.yml      #   Deklarasi 7 source table
│       │   └── stg_*.sql        #   7 staging model
│       ├── intermediate/
│       │   └── int_po_lifecycle.sql  # JOIN PO + GR → lifecycle lengkap
│       └── marts/
│           ├── schema.yml       #   127 dbt tests
│           ├── dim_date.sql     #   Dimensi tanggal (2018–2027)
│           ├── dim_departments.sql   # Dimensi departemen (SCD Type 0)
│           ├── dim_items.sql    #   Dimensi item (SCD Type 1)
│           ├── dim_vendors.sql  #   Dimensi vendor (SCD Type 2)
│           └── fct_purchase_order_lines.sql  # Fakta (Accumulating Snapshot)
│
├── queries/
│   ├── analytics_verifications.sql   # 5 kueri bisnis + bonus smoke test
│   └── run_verifications.py          # Script verifikasi one-command (PASS/FAIL)
│
├── logs/                        # [IGNORED] dbt.log runtime
├── .gitignore
└── README.md
```

---

## Quick Start

### Prasyarat

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) ≥ 4.x
- [VS Code](https://code.visualstudio.com/) + ekstensi [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

### 1. Buka devcontainer

```bash
# Clone / buka folder di VS Code
# Tekan F1 → "Dev Containers: Reopen in Container"
# Tunggu build selesai (~3–5 menit pertama kali)
```

Saat container selesai di-build, `postCreateCommand` otomatis:
- Membuat skema `raw` dan `analytics` di PostgreSQL
- Menjalankan `airflow db init`
- Membuat user Airflow admin (`admin` / `admin`)

### 2. Buka Airflow UI

Buka browser: **http://localhost:8080**

| Field | Value |
|---|---|
| Username | `admin` |
| Password | `admin` |

### 3. Jalankan pipeline

Di Airflow UI, aktifkan DAG **`procurement_pipeline`** lalu klik **▶ Trigger DAG**.

Atau via terminal di dalam container:

```bash
airflow dags trigger procurement_pipeline
```

---

## Menjalankan Pipeline

### DAG: `procurement_pipeline`

Schedule: `@daily` | Catchup: `False` | Timeout per task: 30–60 menit

```
generate_data  →  ingest_to_postgres  →  dbt_run  →  dbt_test
```

| Task | Operator | Deskripsi |
|---|---|---|
| `generate_data` | PythonOperator | Jalankan `data_generator/generate_data.py`, hasilkan 7 CSV |
| `ingest_to_postgres` | PythonOperator | Full load (TRUNCATE + INSERT) ke skema `raw` via psycopg2 |
| `dbt_run` | BashOperator | `dbt run` — build semua 13 model (staging → intermediate → marts) |
| `dbt_test` | BashOperator | `dbt test` — jalankan 127 data quality tests |

### Menjalankan dbt secara manual

```bash
cd /workspaces/airflow-dbt/procurement_dw

# Build semua model
dbt run --profiles-dir . --target dev --no-version-check

# Build layer tertentu saja
dbt run --profiles-dir . --target dev --select staging
dbt run --profiles-dir . --target dev --select marts

# Jalankan tests
dbt test --profiles-dir . --target dev --no-version-check

# Test model tertentu saja
dbt test --profiles-dir . --target dev --select marts.fct_purchase_order_lines
```

---

## Data Model

### Star Schema

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  date_key   │◄──────────────────┐
                    └─────────────┘                   │ (×3: po, delivery, gr)
                                                      │
┌──────────────┐   ┌────────────────────────────────────────────────┐   ┌──────────────────┐
│ dim_vendors  │   │          fct_purchase_order_lines              │   │  dim_departments │
│  (SCD 2)     │◄──│                                                │──►│   (SCD 0)        │
└──────────────┘   │  Grain : 1 baris per PO line                   │   └──────────────────┘
                   │  Pola  : Accumulating Snapshot                 │
┌──────────────┐   │                                                │   ┌──────────────────┐
│  dim_items   │◄──│  Milestone M1: po_date                         │   │                  │
│  (SCD 1)     │   │  Milestone M2: delivery_date_expected          │   │                  │
└──────────────┘   │  Milestone M3: gr_date (NULL = belum diterima) │   │                  │
                   └────────────────────────────────────────────────┘   └──────────────────┘
```

### Tabel & Ukuran Data

| Tabel | Skema | Baris | Pola | Keterangan |
|---|---|---|---|---|
| `dim_date` | marts | 3.652 | SCD Type 0 | Kalender 2018–2027 |
| `dim_departments` | marts | 30 | SCD Type 0 | Departemen statis |
| `dim_items` | marts | 300 | SCD Type 1 | Katalog barang (overwrite) |
| `dim_vendors` | marts | 169 | SCD Type 2 | Vendor + histori perubahan |
| `fct_purchase_order_lines` | marts | 10.821 | Accumulating Snapshot | PO line utama |

### Sumber Data Sintetis (`raw.*`)

| Tabel | Baris | Keterangan |
|---|---|---|
| `departments` | 30 | Master departemen |
| `vendors` | 150 | Vendor aktif + non-aktif |
| `items` | 300 | Katalog barang/jasa (8 kategori) |
| `purchase_requests` | 2.500 | Header PR |
| `purchase_orders` | 2.133 | Header PO (subset PR yang disetujui) |
| `purchase_order_lines` | 10.821 | **Tabel transaksi utama** |
| `goods_receipts` | 8.174 | GR — ~75% PO line sudah diterima |

---

## Lapisan dbt

### Staging (`staging.*`)

View 1:1 dari tabel raw, dengan:
- Rename kolom ke snake_case konsisten
- Cast tipe data (DATE, BOOLEAN, NUMERIC)
- Tambah kolom `loaded_at` (timestamp ingest)

```
stg_departments, stg_vendors, stg_items,
stg_purchase_requests, stg_purchase_orders,
stg_purchase_order_lines, stg_goods_receipts
```

### Intermediate (`intermediate.*`)

```sql
int_po_lifecycle  -- INNER JOIN pol→po; LEFT JOIN gr
                  -- Hitung semua measures & flags lifecycle
```

Menghasilkan 1 baris per PO line dengan seluruh kalkulasi:
lead time, fulfillment %, shortfall IDR, flags (is_on_time, is_short_delivery, dll.)

### Marts (`marts.*`)

| Model | Tipe | Surrogate Key |
|---|---|---|
| `dim_date` | Table | `date_key` = YYYYMMDD integer |
| `dim_departments` | Table | `department_sk` = MD5(department_id) |
| `dim_items` | Table | `item_sk` = MD5(item_id) |
| `dim_vendors` | Table | `vendor_sk` = MD5(vendor_id \|\| '\|' \|\| dbt_valid_from) |
| `fct_purchase_order_lines` | Table | `fct_po_line_sk` = MD5(po_line_id) |

**SCD Type 2 — `dim_vendors`**

Kolom yang dilacak: `vendor_name`, `city`, `province`, `payment_terms`

```sql
-- Ambil record current saja (untuk join ke fakta)
SELECT * FROM marts.dim_vendors WHERE is_current_record = TRUE;

-- Lihat histori perubahan vendor tertentu
SELECT vendor_id, vendor_name, city, dbt_valid_from, dbt_valid_to, record_version
FROM marts.dim_vendors
WHERE vendor_id = 42
ORDER BY dbt_valid_from;
```

---

## Verifikasi Data

### Opsi 1 — Python runner (rekomendasi, one-command)

```bash
cd /workspaces/airflow-dbt
python queries/run_verifications.py
```

Output: 11 smoke tests (✅/❌) + hasil 5 query bisnis.
Exit code `0` = semua lulus, `1` = ada yang gagal.

### Opsi 2 — dbt test (127 structural tests)

```bash
cd /workspaces/airflow-dbt/procurement_dw
dbt test --profiles-dir . --target dev --no-version-check \
  2>&1 | tee /tmp/dbt_test.log; grep -q 'Completed successfully' /tmp/dbt_test.log
```

### Opsi 3 — Query manual via psql

```bash
psql postgresql://admin:admin@postgres:5432/procurement_dw \
  -f queries/analytics_verifications.sql
```

### 5 Pertanyaan Bisnis yang Dijawab

| # | Pertanyaan | Tabel Utama |
|---|---|---|
| Q1 | Total nilai PO (neto) per bulan | `fct` |
| Q2 | Top 5 vendor berdasarkan volume transaksi | `fct` + `dim_vendors` |
| Q3 | Rata-rata lead time per kategori item | `fct` + `dim_items` |
| Q4 | Distribusi ketidaksesuaian kuantitas (shortfall) | `fct` |
| Q5 | Total pengeluaran vs anggaran per departemen | `fct` + `dim_departments` |

---

## Melihat Lineage Graph

### Opsi 1 — VS Code dbt Power User (langsung di editor, tanpa server)

Extension `innoverio.vscode-dbt-power-user` sudah ter-install di devcontainer.

1. Buka file `.sql` model mana saja di `procurement_dw/models/`
   (mis. [`fct_purchase_order_lines.sql`](procurement_dw/models/marts/fct_purchase_order_lines.sql))
2. Klik kanan di dalam editor → **"View Lineage"**
   _atau_ klik ikon lineage (rantai) di toolbar kanan atas editor
3. Panel lineage muncul di samping — menampilkan upstream & downstream model secara interaktif

> **Tips:** Klik node di panel lineage untuk langsung membuka model tersebut.
> Extension membaca `dbt_project.yml` dan `manifest.json` dari folder `target/` secara otomatis.

---

### Opsi 2 — dbt Docs (browser, full DAG interaktif)

Port yang digunakan: **8081** (port 8080 sudah dipakai Airflow).

```bash
cd /workspaces/airflow-dbt/procurement_dw

# Step 1: generate catalog (diperlukan sekali; ulangi setelah dbt run)
dbt docs generate --profiles-dir . --target dev --no-version-check \
  2>&1 | tee /tmp/dbt_docs.log; grep -q 'Catalog written to' /tmp/dbt_docs.log

# Step 2: jalankan web server (buka tab terminal baru, Ctrl+C untuk stop)
dbt docs serve --profiles-dir . --port 8081
```

Buka browser: **http://localhost:8081**

Navigasi lineage:
1. Klik nama model di sidebar kiri
2. Pilih tab **"Lineage"** di panel kanan
3. Gunakan tombol **`+` / `−`** untuk expand upstream/downstream nodes

> **Catatan protobuf:** `dbt docs generate` terkena bug yang sama seperti `dbt run`
> (traceback protobuf di akhir). File `catalog.json` tetap ditulis dengan benar —
> gunakan `grep -q 'Catalog written to'` untuk memverifikasi keberhasilan, bukan exit code.

---

## Konfigurasi

### Koneksi Database

| Parameter | Value |
|---|---|
| Host (dalam container) | `postgres` |
| Port | `5432` |
| Database | `procurement_dw` |
| User / Password | `admin` / `admin` |
| Connection string | `postgresql://admin:admin@postgres:5432/procurement_dw` |

### Airflow Environment Variables (di-set otomatis via `devcontainer.json`)

```
AIRFLOW_HOME                        = /workspaces/airflow-dbt/airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = postgresql+psycopg2://admin:admin@postgres:5432/procurement_dw
AIRFLOW__CORE__EXECUTOR             = LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES        = False
```

### dbt Profiles (`procurement_dw/profiles.yml`)

```yaml
procurement_dw:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      port: 5432
      dbname: procurement_dw
      user: admin
      password: admin
      schema: staging   # default schema (di-override per layer via macro)
      threads: 4
```

---

## Known Issues

### dbt 1.7.0 + protobuf ≥ 4 (non-fatal)

**Gejala:** Setelah `dbt run` / `dbt test` **berhasil**, muncul traceback:

```
TypeError: MessageToJson() got an unexpected keyword argument 'including_default_value_fields'
Command exited with return code 1
```

**Penyebab:** dbt 1.7.0 menggunakan API protobuf 3.x untuk post-run resource report,
sementara protobuf yang terinstall adalah versi 6.x. Crash terjadi *setelah* semua model
selesai diproses — tidak ada model yang gagal.

**Workaround (sudah diterapkan di DAG):** Bash command di task `dbt_run` dan `dbt_test`
menggunakan pola `tee + grep 'Completed successfully'` — memeriksa output dbt, bukan
exit code-nya:

```bash
dbt run ... 2>&1 | tee /tmp/dbt_run_output.log; \
grep -q 'Completed successfully' /tmp/dbt_run_output.log
```

> Downgrade protobuf tidak dilakukan karena `googleapis-common-protos` dan
> `opentelemetry-proto` juga membutuhkan protobuf versi baru.
