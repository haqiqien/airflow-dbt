"""
=============================================================================
DAG: procurement_pipeline
=============================================================================
Pipeline ETL domain Procurement yang mengotomatiskan alur data end-to-end.

ALUR TASK
─────────
  generate_data  ──►  ingest_to_postgres  ──►  dbt_run  ──►  dbt_test

DETAIL TASK
───────────
  1. generate_data       [BashOperator]
       Menjalankan skrip data_generator/generate_data.py untuk membuat 7 tabel
       CSV sintetis di folder ./data/.

  2. ingest_to_postgres  [PythonOperator]
       Full Load: membaca setiap CSV, lalu melakukan TRUNCATE + INSERT ke
       skema "raw" di PostgreSQL. Urutan load memperhatikan dependensi FK.
       Melaporkan ringkasan baris per tabel via XCom.

  3. dbt_run             [BashOperator]
       Menjalankan `dbt run` untuk mentransformasi data dari skema raw ke
       lapisan marts/core di dalam proyek dbt.

  4. dbt_test            [BashOperator]
       Menjalankan `dbt test` untuk memvalidasi kualitas data hasil transformasi.

KONFIGURASI
───────────
  Sesuaikan konstanta di blok CONFIG di bawah sebelum menjalankan DAG.
  Untuk dbt, pastikan dbt_project.yml dan profiles.yml sudah tersedia
  di DBT_PROJECT_DIR sebelum task dbt_run dijalankan.

RETRY LOGIC
───────────
  generate_data      : retries=2, delay=2m  (deterministik, cepat pulih)
  ingest_to_postgres : retries=3, delay=5m, exponential backoff (I/O network)
  dbt_run            : retries=2, delay=5m  (idempoten, re-run aman)
  dbt_test           : retries=1, delay=3m  (jika test gagal, umumnya memang ada masalah data)
=============================================================================
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# =============================================================================
# AIRFLOW DATASETS – merepresentasikan output dari setiap stage pipeline
# URI berfungsi sebagai identifier unik untuk tracking data lineage di UI.
# =============================================================================
DATASET_CSV    = Dataset("file:///workspaces/airflow-dbt/data/")
DATASET_RAW_PG = Dataset("postgresql://postgres:5432/procurement_dw/raw")
DATASET_MARTS  = Dataset("postgresql://postgres:5432/procurement_dw/analytics")

# =============================================================================
# CONFIG – sesuaikan path dan koneksi di sini
# =============================================================================
WORKSPACE_DIR   = "/workspaces/airflow-dbt"
DATA_DIR        = os.path.join(WORKSPACE_DIR, "data")
GENERATOR_SCRIPT= os.path.join(WORKSPACE_DIR, "data_generator", "generate_data.py")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR",  os.path.join(WORKSPACE_DIR, "procurement_dw"))
DBT_PROFILES_DIR= os.getenv("DBT_PROFILES_DIR", os.path.join(WORKSPACE_DIR, "procurement_dw"))

# Koneksi PostgreSQL (bisa juga diganti dengan Airflow Connection ID)
PG_CONN_STRING  = "postgresql+psycopg2://admin:admin@postgres:5432/procurement_dw"
PG_SCHEMA       = "raw"

# Urutan load penting: tabel referensi (parent) harus masuk sebelum tabel transaksi (child)
# agar tidak ada masalah saat FK dibuat di layer downstream.
TABLE_LOAD_ORDER = [
    "departments",           # tidak ada FK → masuk pertama
    "vendors",               # tidak ada FK
    "items",                 # tidak ada FK
    "purchase_requests",     # FK → departments
    "purchase_orders",       # FK → purchase_requests, vendors, departments
    "purchase_order_lines",  # FK → purchase_orders, items
    "goods_receipts",        # FK → purchase_orders, purchase_order_lines, items
]

# =============================================================================
# CALLABLE: ingest_to_postgres
# =============================================================================

def _parse_pg_dsn(conn_str: str) -> dict:
    """
    Parse connection string PostgreSQL ke dict parameter psycopg2.
    Format: postgresql+psycopg2://user:password@host:port/dbname
    """
    # Hapus prefix driver agar urllib bisa parse
    url = conn_str.replace("postgresql+psycopg2://", "postgresql://")
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return {
        "host"    : parsed.hostname,
        "port"    : parsed.port or 5432,
        "dbname"  : parsed.path.lstrip("/"),
        "user"    : parsed.username,
        "password": parsed.password,
    }


def _pg_col_type(dtype, series) -> str:
    """
    Map pandas dtype ke tipe kolom PostgreSQL.
    Kolom object diperiksa isinya untuk membedakan DATE vs TEXT.
    """
    import pandas as pd
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    # object → coba deteksi DATE dari sample non-null
    sample = series.dropna().head(5).tolist()
    if sample:
        import re
        date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        if all(isinstance(v, str) and date_re.match(str(v)) for v in sample):
            return "DATE"
    return "TEXT"


def ingest_csv_to_postgres(**context) -> dict:
    """
    Full Load setiap tabel CSV ke PostgreSQL skema `raw`.

    Menggunakan psycopg2 secara langsung (tanpa pandas.to_sql) karena:
    - pandas 2.3.x mensyaratkan SQLAlchemy >= 2.0 untuk to_sql
    - Environment ini menggunakan SQLAlchemy 1.4.x
    - psycopg2 + execute_values lebih cepat untuk bulk insert

    Strategi per tabel:
    - Jika tabel SUDAH ADA  → TRUNCATE RESTART IDENTITY, lalu INSERT
    - Jika tabel BELUM ADA  → CREATE TABLE (DDL dari dtype pandas), lalu INSERT
    - Setiap tabel di-commit terpisah: kegagalan 1 tabel tidak rollback tabel lain.
    - NaN/None dikonversi ke NULL sebelum insert.

    Returns:
        dict  {table_name: {"rows": int, "status": str, "elapsed_s": float}}
    """
    import pandas as pd
    import psycopg2
    from psycopg2.extras import execute_values

    dsn     = _parse_pg_dsn(PG_CONN_STRING)
    summary: dict = {}

    pg = psycopg2.connect(**dsn)
    pg.autocommit = False
    cur = pg.cursor()

    # ------------------------------------------------------------------
    # 1. Pastikan skema raw tersedia
    # ------------------------------------------------------------------
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{PG_SCHEMA}"')
    pg.commit()
    log.info("Schema '%s' siap.", PG_SCHEMA)

    # ------------------------------------------------------------------
    # 2. Load setiap tabel
    # ------------------------------------------------------------------
    for table_name in TABLE_LOAD_ORDER:
        filepath = os.path.join(DATA_DIR, f"{table_name}.csv")
        t_start  = time.perf_counter()

        if not os.path.exists(filepath):
            log.warning("File tidak ditemukan: %s — dilewati.", filepath)
            summary[table_name] = {"rows": 0, "status": "SKIPPED", "elapsed_s": 0.0}
            continue

        try:
            # ── Baca CSV ────────────────────────────────────────────────
            df = pd.read_csv(filepath, encoding="utf-8-sig")
            n_rows = len(df)
            log.info("→ %s: %d baris dibaca dari %s", table_name, n_rows, filepath)

            # Tambah kolom audit waktu ingest
            df["_ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            full_table = f'"{PG_SCHEMA}"."{table_name}"'

            # ── Cek apakah tabel sudah ada ───────────────────────────────
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (PG_SCHEMA, table_name))
            table_exists = cur.fetchone()[0]

            if table_exists:
                cur.execute(f"TRUNCATE TABLE {full_table} RESTART IDENTITY")
                pg.commit()
                log.info("   TRUNCATE %s", full_table)
            else:
                # ── Buat tabel baru dengan DDL dari dtype DataFrame ────────
                col_defs = ", ".join(
                    f'"{col}" {_pg_col_type(dtype, df[col])}'
                    for col, dtype in df.dtypes.items()
                )
                cur.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({col_defs})")
                pg.commit()
                log.info("   CREATE TABLE %s", full_table)

            # ── Bulk insert dengan execute_values ─────────────────────────
            # NaN → None agar psycopg2 insert sebagai NULL
            df_clean = df.where(df.notna(), other=None)
            records  = [tuple(row) for row in df_clean.itertuples(index=False, name=None)]
            col_list = ", ".join(f'"{c}"' for c in df.columns)
            insert_sql = f"INSERT INTO {full_table} ({col_list}) VALUES %s"

            execute_values(cur, insert_sql, records, page_size=1_000)
            pg.commit()

            elapsed = time.perf_counter() - t_start
            log.info("   ✓ %s: %d baris → %s  (%.2fs)", table_name, n_rows, full_table, elapsed)
            summary[table_name] = {"rows": n_rows, "status": "OK", "elapsed_s": round(elapsed, 2)}

        except Exception as exc:  # noqa: BLE001
            pg.rollback()
            elapsed = time.perf_counter() - t_start
            log.error("   ✗ %s GAGAL: %s", table_name, exc, exc_info=True)
            summary[table_name] = {"rows": 0, "status": f"ERROR: {exc}", "elapsed_s": round(elapsed, 2)}
            raise  # propagate → Airflow tandai task FAILED → trigger retry

    cur.close()
    pg.close()

    # ------------------------------------------------------------------
    # 3. Cetak ringkasan & push ke XCom
    # ------------------------------------------------------------------
    total_rows = sum(v["rows"] for v in summary.values())
    log.info("=" * 55)
    log.info("  RINGKASAN INGEST")
    log.info("=" * 55)
    for tbl, info in summary.items():
        log.info("  %-28s %6s baris  [%s]  %.2fs",
                 tbl, f"{info['rows']:,}", info["status"], info["elapsed_s"])
    log.info("  %-28s %6s baris total", "GRAND TOTAL", f"{total_rows:,}")
    log.info("=" * 55)

    # Simpan ke XCom agar task downstream bisa membaca (opsional)
    context["ti"].xcom_push(key="ingest_summary", value=summary)
    context["ti"].xcom_push(key="total_rows_ingested", value=total_rows)

    return summary


# =============================================================================
# DEFAULT ARGS
# =============================================================================
default_args = {
    "owner"                  : "data-engineer",
    "depends_on_past"        : False,
    "email_on_failure"       : False,
    "email_on_retry"         : False,
    # Retry default — di-override per task bila perlu
    "retries"                : 3,
    "retry_delay"            : timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5m → 10m → 20m  (capped di max_retry_delay)
    "max_retry_delay"        : timedelta(minutes=30),
    "execution_timeout"      : timedelta(hours=1),
}

# =============================================================================
# DAG DEFINITION
# =============================================================================
with DAG(
    dag_id           = "procurement_pipeline",
    description      = "ETL Pipeline Procurement: Generate Data → Ingest PostgreSQL → dbt run → dbt test",
    schedule_interval= "@daily",
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    max_active_runs  = 1,          # hindari pipeline concurrent yang berebut resource
    dagrun_timeout   = timedelta(hours=3),
    default_args     = default_args,
    tags             = ["procurement", "etl", "dbt", "raw"],
    doc_md           = """
## 🏗️ Procurement ETL Pipeline

Pipeline harian yang mengotomatiskan alur data Procurement end-to-end.

### Task Flow
```
generate_data ──► ingest_to_postgres ──► dbt_run ──► dbt_test
```

### Persyaratan Sebelum Menjalankan
- Postgres aktif dan bisa dijangkau di `postgres:5432`
- Folder `./data/` dapat ditulis oleh proses Airflow
- Proyek dbt tersedia di `DBT_PROJECT_DIR` (untuk task dbt_run & dbt_test)

### Output
- **`raw.*`** : 7 tabel staging di PostgreSQL (hasil ingest CSV)
- **marts/core** : tabel hasil transformasi dbt (setelah dbt_run)
""",
) as dag:

    # ──────────────────────────────────────────────────────────────────
    # TASK 1 ─ generate_data
    # ──────────────────────────────────────────────────────────────────
    t_generate = BashOperator(
        task_id   = "generate_data",
        bash_command = (
            f"echo '[generate_data] Memulai generasi data sintetis...' && "
            f"python {GENERATOR_SCRIPT} && "
            f"echo '[generate_data] Selesai. File CSV tersedia di {DATA_DIR}'"
        ),
        outlets           = [DATASET_CSV],   # menghasilkan file CSV di ./data/
        # Generate data lebih cepat pulih; tidak perlu 3x retry
        retries           = 2,
        retry_delay       = timedelta(minutes=2),
        retry_exponential_backoff = False,
        execution_timeout = timedelta(minutes=15),
        doc_md            = "Menjalankan `generate_data.py` untuk membuat 7 tabel CSV sintetis.",
    )

    # ──────────────────────────────────────────────────────────────────
    # TASK 2 ─ ingest_to_postgres
    # ──────────────────────────────────────────────────────────────────
    t_ingest = PythonOperator(
        task_id         = "ingest_to_postgres",
        python_callable = ingest_csv_to_postgres,
        inlets            = [DATASET_CSV],      # mengkonsumsi file CSV dari generate_data
        outlets           = [DATASET_RAW_PG],   # menghasilkan tabel di skema raw PostgreSQL
        # provide_context sudah default True di Airflow 2.x bila ada **context
        retries           = 3,
        retry_delay       = timedelta(minutes=5),
        retry_exponential_backoff = True,
        max_retry_delay   = timedelta(minutes=30),
        execution_timeout = timedelta(hours=1),
        doc_md            = (
            "Full Load: TRUNCATE + INSERT semua tabel CSV ke PostgreSQL skema `raw`. "
            "Laporan per tabel tersedia di XCom key `ingest_summary`."
        ),
    )

    # ──────────────────────────────────────────────────────────────────
    # TASK 3 ─ dbt_run
    # ──────────────────────────────────────────────────────────────────
    t_dbt_run = BashOperator(
        task_id      = "dbt_run",
        bash_command = (
            "echo '[dbt_run] Memulai transformasi dbt...' && "
            f"cd {DBT_PROJECT_DIR} && "
            # dbt 1.7.0 + protobuf >=4 bug: dbt exits with code 1 after a successful run
            # due to MessageToJson() incompatibility in the post-run Resource report.
            # All models complete correctly before the crash. We pipe output to a temp
            # file and use grep to check the real outcome instead of the (buggy) exit code.
            f"dbt run "
            f"  --profiles-dir {DBT_PROFILES_DIR} "
            f"  --target dev "
            f"  --no-version-check "
            f"  2>&1 | tee /tmp/dbt_run_output.log; "
            "grep -q 'Completed successfully' /tmp/dbt_run_output.log && "
            "echo '[dbt_run] Transformasi selesai.'"
        ),
        inlets            = [DATASET_RAW_PG],   # mengkonsumsi tabel raw dari ingest_to_postgres
        outlets           = [DATASET_MARTS],     # menghasilkan tabel marts hasil transformasi dbt
        env = {
            # Teruskan semua env var yang ada + override khusus dbt
            **{k: v for k, v in os.environ.items()},
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
        },
        retries           = 2,
        retry_delay       = timedelta(minutes=5),
        retry_exponential_backoff = False,
        execution_timeout = timedelta(minutes=30),
        doc_md            = (
            f"Menjalankan `dbt run` di `{DBT_PROJECT_DIR}` "
            "untuk mentransformasi data dari skema `raw` ke lapisan marts."
        ),
    )

    # ──────────────────────────────────────────────────────────────────
    # TASK 4 ─ dbt_test
    # ──────────────────────────────────────────────────────────────────
    t_dbt_test = BashOperator(
        task_id      = "dbt_test",
        bash_command = (
            "echo '[dbt_test] Memulai validasi kualitas data...' && "
            f"cd {DBT_PROJECT_DIR} && "
            # Same protobuf workaround as dbt_run: check for 'Completed successfully'
            # in output instead of relying on the (bugged) exit code.
            f"dbt test "
            f"  --profiles-dir {DBT_PROFILES_DIR} "
            f"  --target dev "
            f"  --no-version-check "
            f"  2>&1 | tee /tmp/dbt_test_output.log; "
            "grep -q 'Completed successfully' /tmp/dbt_test_output.log && "
            "echo '[dbt_test] Semua test lulus.'"
        ),
        inlets            = [DATASET_MARTS],    # memvalidasi tabel marts hasil dbt_run
        env = {
            **{k: v for k, v in os.environ.items()},
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
        },
        # dbt test yang gagal biasanya memang ada masalah data → 1 retry cukup
        retries           = 1,
        retry_delay       = timedelta(minutes=3),
        retry_exponential_backoff = False,
        execution_timeout = timedelta(minutes=20),
        # Jalankan test meski dbt_run gagal? Tidak. Default TriggerRule.ALL_SUCCESS sudah benar.
        trigger_rule      = TriggerRule.ALL_SUCCESS,
        doc_md            = (
            f"Menjalankan `dbt test` di `{DBT_PROJECT_DIR}` "
            "untuk memvalidasi kualitas data hasil transformasi."
        ),
    )

    # ──────────────────────────────────────────────────────────────────
    # TASK DEPENDENCIES
    # ──────────────────────────────────────────────────────────────────
    t_generate >> t_ingest >> t_dbt_run >> t_dbt_test
