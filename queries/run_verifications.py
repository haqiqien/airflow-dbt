#!/usr/bin/env python3
"""
run_verifications.py — Verifikasi Data Mart Procurement DW
===========================================================
Menjalankan dua tahap verifikasi terhadap schema `marts`:

  1. Smoke Tests   : row counts + FK orphan checks  (✅ PASS / ❌ FAIL)
  2. Query Bisnis  : 5 kueri analitik, tampilkan hasil ringkas

Penggunaan:
  cd /workspaces/airflow-dbt
  python queries/run_verifications.py

Prasyarat: dbt run sudah berhasil dan PostgreSQL berjalan di localhost:5432.
Exit code : 0 = semua smoke test lulus | 1 = ada yang gagal (CI-friendly).
"""

import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ─── Koneksi ────────────────────────────────────────────────────────────────
PG_CONN = dict(
    host     = "postgres",   # docker-compose service name (sama seperti di DAG)
    port     = 5432,
    dbname   = "procurement_dw",
    user     = "admin",
    password = "admin",
)

# ─── Smoke Tests ─────────────────────────────────────────────────────────────
# Format: (label, SQL yang mengembalikan 1 angka, fungsi pass/fail)
SMOKE_TESTS = [
    (
        "dim_date: 3.652 baris (tahun 2018–2027)",
        "SELECT COUNT(*) FROM marts.dim_date",
        lambda n: n == 3652,
    ),
    (
        "dim_departments: 30 baris",
        "SELECT COUNT(*) FROM marts.dim_departments",
        lambda n: n == 30,
    ),
    (
        "dim_items: 300 baris",
        "SELECT COUNT(*) FROM marts.dim_items",
        lambda n: n == 300,
    ),
    (
        "dim_vendors: 150 current records (satu per vendor_id)",
        "SELECT COUNT(*) FROM marts.dim_vendors WHERE is_current_record = TRUE",
        lambda n: n == 150,
    ),
    (
        "dim_vendors: ~169 semua versi (termasuk historis SCD2)",
        "SELECT COUNT(*) FROM marts.dim_vendors",
        lambda n: 155 <= n <= 185,
    ),
    (
        "fct_purchase_order_lines: ~10.821 baris",
        "SELECT COUNT(*) FROM marts.fct_purchase_order_lines",
        lambda n: 10_000 <= n <= 12_000,
    ),
    (
        "fct: ~8.174 baris dengan GR (has_goods_receipt = TRUE)",
        "SELECT COUNT(*) FROM marts.fct_purchase_order_lines WHERE has_goods_receipt = TRUE",
        lambda n: 7_500 <= n <= 9_000,
    ),
    (
        "fct: 0 orphan vendor_sk (vendor_sk IS NULL)",
        "SELECT COUNT(*) FROM marts.fct_purchase_order_lines WHERE vendor_sk IS NULL",
        lambda n: n == 0,
    ),
    (
        "fct: 0 orphan date_key_po_date (date FK NULL)",
        "SELECT COUNT(*) FROM marts.fct_purchase_order_lines WHERE date_key_po_date IS NULL",
        lambda n: n == 0,
    ),
    (
        "fct: 0 orphan item_sk (item_sk IS NULL)",
        "SELECT COUNT(*) FROM marts.fct_purchase_order_lines WHERE item_sk IS NULL",
        lambda n: n == 0,
    ),
    (
        "fct: 0 orphan department_sk (department_sk IS NULL)",
        "SELECT COUNT(*) FROM marts.fct_purchase_order_lines WHERE department_sk IS NULL",
        lambda n: n == 0,
    ),
]

# ─── Query Bisnis ─────────────────────────────────────────────────────────────
# Format: (judul, SQL)
BUSINESS_QUERIES = [
    (
        "Q1 — Total Nilai PO per Bulan (5 bulan terakhir)",
        """
        SELECT
            f.po_year_month                                      AS tahun_bulan,
            COUNT(DISTINCT f.po_id)                              AS jumlah_po,
            COUNT(f.po_line_id)                                  AS jumlah_line,
            ROUND(SUM(f.net_total_ordered_idr) / 1e9, 2)        AS total_neto_miliar_idr,
            ROUND(SUM(f.discount_amount_idr)   / 1e9, 2)        AS total_diskon_miliar_idr
        FROM marts.fct_purchase_order_lines f
        WHERE f.is_cancelled = FALSE
        GROUP BY f.po_year_month
        ORDER BY f.po_year_month DESC
        LIMIT 5
        """,
    ),
    (
        "Q2 — Top 5 Vendor berdasarkan Total Nilai Transaksi",
        """
        SELECT
            v.vendor_name,
            v.vendor_category,
            COUNT(f.po_line_id)                                  AS jumlah_line,
            ROUND(SUM(f.net_total_ordered_idr) / 1e9, 2)        AS total_neto_miliar_idr,
            ROUND(100.0 * AVG(f.fulfillment_pct), 1)            AS avg_fulfillment_pct,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE f.is_on_time)
                      / NULLIF(COUNT(*) FILTER (WHERE f.has_goods_receipt), 0)
            , 1)                                                 AS pct_on_time
        FROM marts.fct_purchase_order_lines f
        JOIN marts.dim_vendors v ON f.vendor_sk = v.vendor_sk
        WHERE f.is_cancelled = FALSE
        GROUP BY v.vendor_name, v.vendor_category
        ORDER BY SUM(f.net_total_ordered_idr) DESC
        LIMIT 5
        """,
    ),
    (
        "Q3 — Rata-rata Lead Time per Kategori Item",
        """
        SELECT
            i.item_category,
            COUNT(*)                                             AS jumlah_line_gr,
            ROUND(AVG(f.expected_lead_time_days), 1)            AS avg_expected_days,
            ROUND(AVG(f.actual_lead_time_days),   1)            AS avg_actual_days,
            ROUND(AVG(f.lead_time_variance_days), 1)            AS avg_variance_days,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE f.is_on_time)
                      / NULLIF(COUNT(*), 0)
            , 1)                                                 AS pct_on_time
        FROM marts.fct_purchase_order_lines f
        JOIN marts.dim_items i ON f.item_sk = i.item_sk
        WHERE f.has_goods_receipt = TRUE
        GROUP BY i.item_category
        ORDER BY AVG(f.actual_lead_time_days) DESC
        """,
    ),
    (
        "Q4 — Distribusi Pemenuhan Kuantitas (receipt_status)",
        """
        SELECT
            receipt_status,
            COUNT(*)                                             AS jumlah_line,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)  AS pct,
            ROUND(SUM(value_shortfall_idr) / 1e9, 2)            AS shortfall_miliar_idr
        FROM marts.fct_purchase_order_lines
        WHERE is_cancelled = FALSE
        GROUP BY receipt_status
        ORDER BY COUNT(*) DESC
        """,
    ),
    (
        "Q5 — Top 5 Departemen berdasarkan Realisasi Pengeluaran",
        """
        SELECT
            d.department_name,
            ROUND(SUM(f.net_total_ordered_idr)  / 1e9, 2)      AS ordered_miliar_idr,
            ROUND(SUM(f.net_total_received_idr) / 1e9, 2)      AS received_miliar_idr,
            ROUND(d.budget_annual_idr           / 1e9, 2)      AS budget_miliar_idr,
            ROUND(
                100.0 * SUM(f.net_total_received_idr)
                      / NULLIF(d.budget_annual_idr, 0)
            , 1)                                                 AS realisasi_pct
        FROM marts.fct_purchase_order_lines f
        JOIN marts.dim_departments d ON f.department_sk = d.department_sk
        WHERE f.is_cancelled = FALSE
        GROUP BY d.department_name, d.budget_annual_idr
        ORDER BY SUM(f.net_total_received_idr) DESC
        LIMIT 5
        """,
    ),
]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _col_widths(headers: list, rows: list) -> list[int]:
    """Lebar kolom = max(panjang header, panjang nilai terpanjang di kolom itu)."""
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val) if val is not None else "NULL"))
    return widths


def print_table(headers: list, rows: list) -> None:
    if not rows:
        print("    (tidak ada data)")
        return
    widths = _col_widths(headers, rows)
    sep    = "  ".join("-" * w for w in widths)
    fmt    = lambda cells: "  ".join(str(c if c is not None else "NULL").ljust(w) for c, w in zip(cells, widths))
    print("    " + fmt(headers))
    print("    " + sep)
    for row in rows:
        print("    " + fmt(row))


def hr(char: str = "─", width: int = 70) -> str:
    return char * width


# ─── Runner ──────────────────────────────────────────────────────────────────

def run_smoke_tests(conn) -> int:
    """Kembalikan jumlah test yang GAGAL."""
    fail_count = 0
    print(f"\n{'▶'} SMOKE TESTS  (row counts + FK integrity)\n")

    with conn.cursor() as cur:
        for label, sql, check_fn in SMOKE_TESTS:
            cur.execute(sql)
            n   = cur.fetchone()[0]
            ok  = check_fn(n)
            if ok:
                icon = "✅ PASS"
            else:
                icon = "❌ FAIL"
                fail_count += 1
            print(f"  {icon}  {label}  [{n:,}]")

    return fail_count


def run_business_queries(conn) -> None:
    print(f"\n{hr()}")
    print(f"{'▶'} QUERY BISNIS\n")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for title, sql in BUSINESS_QUERIES:
            print(f"\n  📊 {title}")
            print("  " + hr("─", 60))
            cur.execute(sql)
            rows = cur.fetchall()
            if rows:
                headers = list(rows[0].keys())
                data    = [list(row.values()) for row in rows]
                print_table(headers, data)
            else:
                print("    (tidak ada data)")


def main() -> None:
    # ── Sambung ke PostgreSQL ─────────────────────────────────────────────
    try:
        conn = psycopg2.connect(**PG_CONN)
    except Exception as exc:
        print(f"\n❌  Gagal konek ke PostgreSQL: {exc}")
        print("    Pastikan container postgres berjalan dan kredensial benar.\n")
        sys.exit(1)

    # ── Header ───────────────────────────────────────────────────────────
    print("\n" + hr("="))
    print("  VERIFIKASI DATA MART — PROCUREMENT DW")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(hr("="))

    try:
        fail_count = run_smoke_tests(conn)
        run_business_queries(conn)
    finally:
        conn.close()

    # ── Ringkasan ────────────────────────────────────────────────────────
    total = len(SMOKE_TESTS)
    print(f"\n{hr('=')}")
    if fail_count == 0:
        print(f"  ✅  SEMUA {total} SMOKE TEST LULUS — data mart siap digunakan.")
    else:
        print(f"  ⚠️   {fail_count}/{total} SMOKE TEST GAGAL — periksa output di atas.")
    print(hr("=") + "\n")

    sys.exit(1 if fail_count > 0 else 0)


if __name__ == "__main__":
    main()
