#!/usr/bin/env python3
"""
run_verifications.py — Verifikasi Data Mart Procurement DW
===========================================================
Menjalankan dua tahap verifikasi terhadap schema `marts`:

  1. Smoke Tests   : row counts + FK orphan checks  (✅ PASS / ❌ FAIL)
  2. Query Bisnis  : 17 kueri analitik per stakeholder, tampilkan hasil ringkas

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
# Format: (stakeholder, [(judul, SQL), ...])
BUSINESS_QUERIES = [
    (
        "Stakeholder 1 — Manajer Pengadaan (Procurement Manager)",
        [
            (
                "PM1 — Total Nilai PO per Bulan dan Kuartal",
                """
                WITH po_period AS (
                    SELECT
                        'Bulanan'::text                          AS tipe_periode,
                        f.po_year_month                          AS periode,
                        f.po_year_month                          AS sort_periode,
                        COUNT(DISTINCT f.po_id)                  AS jumlah_po,
                        COUNT(f.po_line_id)                      AS jumlah_line,
                        SUM(f.net_total_ordered_idr)             AS total_neto_idr,
                        SUM(f.discount_amount_idr)               AS total_diskon_idr
                    FROM marts.fct_purchase_order_lines f
                    WHERE f.is_cancelled = FALSE
                    GROUP BY f.po_year_month

                    UNION ALL

                    SELECT
                        'Kuartalan'::text                        AS tipe_periode,
                        dd.year_quarter                          AS periode,
                        dd.year_quarter                          AS sort_periode,
                        COUNT(DISTINCT f.po_id)                  AS jumlah_po,
                        COUNT(f.po_line_id)                      AS jumlah_line,
                        SUM(f.net_total_ordered_idr)             AS total_neto_idr,
                        SUM(f.discount_amount_idr)               AS total_diskon_idr
                    FROM marts.fct_purchase_order_lines f
                    JOIN marts.dim_date dd
                        ON f.date_key_po_date = dd.date_key
                    WHERE f.is_cancelled = FALSE
                    GROUP BY dd.year_quarter
                )
                SELECT
                    tipe_periode,
                    periode,
                    jumlah_po,
                    jumlah_line,
                    ROUND(total_neto_idr / 1000000000.0, 2)      AS total_neto_miliar_idr,
                    ROUND(total_diskon_idr / 1000000000.0, 2)    AS total_diskon_miliar_idr
                FROM po_period
                ORDER BY sort_periode DESC, tipe_periode
                LIMIT 12
                """,
            ),
            (
                "PM2 — Vendor dengan Volume Transaksi Terbesar dalam Tahun Terakhir",
                """
                WITH latest_year AS (
                    SELECT MAX(po_year) AS tahun
                    FROM marts.fct_purchase_order_lines
                    WHERE is_cancelled = FALSE
                )
                SELECT
                    f.po_year                                      AS tahun,
                    v.vendor_name,
                    v.vendor_category,
                    COUNT(DISTINCT f.po_id)                        AS jumlah_po,
                    COUNT(f.po_line_id)                            AS jumlah_line,
                    ROUND(SUM(f.net_total_ordered_idr) / 1000000000.0, 2)
                                                                    AS total_neto_miliar_idr,
                    ROUND(AVG(f.fulfillment_pct), 1)               AS avg_fulfillment_pct
                FROM marts.fct_purchase_order_lines f
                JOIN latest_year ly
                    ON f.po_year = ly.tahun
                JOIN marts.dim_vendors v
                    ON f.vendor_sk = v.vendor_sk
                WHERE f.is_cancelled = FALSE
                GROUP BY f.po_year, v.vendor_name, v.vendor_category
                ORDER BY SUM(f.net_total_ordered_idr) DESC
                LIMIT 10
                """,
            ),
            (
                "PM3 — Rata-rata Lead Time PO sampai Goods Receipt",
                """
                SELECT
                    i.item_category,
                    COUNT(*)                                      AS jumlah_line_gr,
                    ROUND(AVG(f.expected_lead_time_days), 1)      AS avg_expected_days,
                    ROUND(AVG(f.actual_lead_time_days), 1)        AS avg_actual_days,
                    ROUND(AVG(f.lead_time_variance_days), 1)      AS avg_variance_days,
                    ROUND(
                        100.0 * COUNT(*) FILTER (WHERE f.is_on_time)
                              / NULLIF(COUNT(*), 0)
                    , 1)                                           AS pct_on_time
                FROM marts.fct_purchase_order_lines f
                JOIN marts.dim_items i
                    ON f.item_sk = i.item_sk
                WHERE f.has_goods_receipt = TRUE
                  AND f.is_cancelled = FALSE
                GROUP BY i.item_category
                ORDER BY AVG(f.actual_lead_time_days) DESC
                """,
            ),
            (
                "PM4 — Persentase PO Line Tepat Waktu vs Terlambat",
                """
                SELECT
                    COALESCE(f.delivery_timeliness, 'Belum Ada GR') AS status_ketepatan,
                    COUNT(*)                                        AS jumlah_line,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)
                                                                     AS pct_dari_total,
                    ROUND(SUM(f.net_total_ordered_idr) / 1000000000.0, 2)
                                                                     AS total_neto_miliar_idr
                FROM marts.fct_purchase_order_lines f
                WHERE f.is_cancelled = FALSE
                GROUP BY COALESCE(f.delivery_timeliness, 'Belum Ada GR')
                ORDER BY COUNT(*) DESC
                """,
            ),
            (
                "PM5 — Kategori Item dengan Kenaikan Harga Signifikan MoM",
                """
                WITH category_month AS (
                    SELECT
                        f.po_year_month,
                        i.item_category,
                        ROUND(AVG(f.net_unit_price_idr), 0)       AS avg_net_unit_price_idr,
                        COUNT(*)                                  AS jumlah_line
                    FROM marts.fct_purchase_order_lines f
                    JOIN marts.dim_items i
                        ON f.item_sk = i.item_sk
                    WHERE f.is_cancelled = FALSE
                    GROUP BY f.po_year_month, i.item_category
                ),
                price_change AS (
                    SELECT
                        po_year_month,
                        item_category,
                        avg_net_unit_price_idr,
                        LAG(avg_net_unit_price_idr)
                            OVER (PARTITION BY item_category ORDER BY po_year_month)
                                                                    AS prev_avg_net_unit_price_idr,
                        jumlah_line
                    FROM category_month
                )
                SELECT
                    po_year_month                                  AS tahun_bulan,
                    item_category,
                    prev_avg_net_unit_price_idr,
                    avg_net_unit_price_idr,
                    ROUND(
                        100.0 * (avg_net_unit_price_idr - prev_avg_net_unit_price_idr)
                              / NULLIF(prev_avg_net_unit_price_idr, 0)
                    , 1)                                           AS pct_kenaikan_harga,
                    jumlah_line
                FROM price_change
                WHERE prev_avg_net_unit_price_idr IS NOT NULL
                  AND avg_net_unit_price_idr > prev_avg_net_unit_price_idr
                ORDER BY pct_kenaikan_harga DESC NULLS LAST
                LIMIT 10
                """,
            ),
        ],
    ),
    (
        "Stakeholder 2 — Tim Keuangan (Finance / Controller)",
        [
            (
                "FIN1 — Aktual vs Budget per Departemen per Bulan (Proxy Budget Bulanan)",
                """
                SELECT
                    f.po_year_month                                AS tahun_bulan,
                    d.department_name,
                    ROUND(SUM(f.net_total_received_idr) / 1000000000.0, 2)
                                                                    AS actual_miliar_idr,
                    ROUND((d.budget_annual_idr / 12.0) / 1000000000.0, 2)
                                                                    AS budget_proxy_miliar_idr,
                    ROUND(
                        100.0 * SUM(f.net_total_received_idr)
                              / NULLIF(d.budget_annual_idr / 12.0, 0)
                    , 1)                                           AS pct_realisasi_budget_proxy
                FROM marts.fct_purchase_order_lines f
                JOIN marts.dim_departments d
                    ON f.department_sk = d.department_sk
                WHERE f.is_cancelled = FALSE
                GROUP BY f.po_year_month, d.department_name, d.budget_annual_idr
                ORDER BY f.po_year_month DESC, SUM(f.net_total_received_idr) DESC
                LIMIT 15
                """,
            ),
            (
                "FIN2 — Estimasi Accounts Payable Outstanding (Proxy Payment Terms)",
                """
                WITH ap_base AS (
                    SELECT
                        v.vendor_name,
                        f.payment_terms,
                        f.gr_date,
                        f.net_total_received_idr,
                        CASE f.payment_terms
                            WHEN 'COD'    THEN 0
                            WHEN 'DP 50%' THEN 15
                            WHEN 'Net 15' THEN 15
                            WHEN 'Net 30' THEN 30
                            WHEN 'Net 45' THEN 45
                            WHEN 'Net 60' THEN 60
                            ELSE 30
                        END                                        AS due_days,
                        (CURRENT_DATE - f.gr_date)::int             AS age_days
                    FROM marts.fct_purchase_order_lines f
                    JOIN marts.dim_vendors v
                        ON f.vendor_sk = v.vendor_sk
                    WHERE f.is_cancelled = FALSE
                      AND f.has_goods_receipt = TRUE
                )
                SELECT
                    vendor_name,
                    payment_terms,
                    COUNT(*)                                        AS jumlah_line_gr,
                    ROUND(SUM(net_total_received_idr) / 1000000000.0, 2)
                                                                     AS outstanding_proxy_miliar_idr,
                    ROUND(SUM(net_total_received_idr) FILTER (WHERE age_days > due_days)
                          / 1000000000.0, 2)                        AS overdue_proxy_miliar_idr,
                    MAX(age_days)                                   AS max_age_days
                FROM ap_base
                GROUP BY vendor_name, payment_terms
                ORDER BY SUM(net_total_received_idr) DESC
                LIMIT 10
                """,
            ),
            (
                "FIN3 — Vendor Paling Sering Memberikan Diskon",
                """
                SELECT
                    v.vendor_name,
                    v.vendor_category,
                    COUNT(*) FILTER (WHERE f.has_discount)          AS jumlah_line_diskon,
                    COUNT(*)                                        AS jumlah_line_total,
                    ROUND(
                        100.0 * COUNT(*) FILTER (WHERE f.has_discount)
                              / NULLIF(COUNT(*), 0)
                    , 1)                                           AS pct_line_diskon,
                    ROUND(SUM(f.discount_amount_idr) / 1000000000.0, 2)
                                                                     AS total_diskon_miliar_idr,
                    ROUND(AVG(f.discount_pct), 2)                  AS avg_discount_pct
                FROM marts.fct_purchase_order_lines f
                JOIN marts.dim_vendors v
                    ON f.vendor_sk = v.vendor_sk
                WHERE f.is_cancelled = FALSE
                GROUP BY v.vendor_name, v.vendor_category
                HAVING COUNT(*) FILTER (WHERE f.has_discount) > 0
                ORDER BY jumlah_line_diskon DESC, SUM(f.discount_amount_idr) DESC
                LIMIT 10
                """,
            ),
            (
                "FIN4 — Proporsi Pengeluaran per Kategori Item",
                """
                SELECT
                    i.item_category,
                    ROUND(SUM(f.net_total_received_idr) / 1000000000.0, 2)
                                                                    AS spend_miliar_idr,
                    ROUND(
                        100.0 * SUM(f.net_total_received_idr)
                              / NULLIF(SUM(SUM(f.net_total_received_idr)) OVER (), 0)
                    , 1)                                           AS pct_total_spend,
                    COUNT(DISTINCT f.po_id)                        AS jumlah_po,
                    COUNT(f.po_line_id)                            AS jumlah_line
                FROM marts.fct_purchase_order_lines f
                JOIN marts.dim_items i
                    ON f.item_sk = i.item_sk
                WHERE f.is_cancelled = FALSE
                GROUP BY i.item_category
                ORDER BY SUM(f.net_total_received_idr) DESC
                """,
            ),
            (
                "FIN5 — Pola Pengeluaran Tidak Wajar per Departemen (Proxy Anomali)",
                """
                WITH monthly_spend AS (
                    SELECT
                        f.po_year_month,
                        d.department_name,
                        SUM(f.net_total_ordered_idr)                AS spend_idr
                    FROM marts.fct_purchase_order_lines f
                    JOIN marts.dim_departments d
                        ON f.department_sk = d.department_sk
                    WHERE f.is_cancelled = FALSE
                    GROUP BY f.po_year_month, d.department_name
                ),
                scored AS (
                    SELECT
                        po_year_month,
                        department_name,
                        spend_idr,
                        AVG(spend_idr) OVER (PARTITION BY department_name)
                                                                    AS avg_spend_idr,
                        STDDEV_POP(spend_idr) OVER (PARTITION BY department_name)
                                                                    AS stddev_spend_idr
                    FROM monthly_spend
                )
                SELECT
                    po_year_month                                  AS tahun_bulan,
                    department_name,
                    ROUND(spend_idr / 1000000000.0, 2)            AS spend_miliar_idr,
                    ROUND(avg_spend_idr / 1000000000.0, 2)        AS baseline_miliar_idr,
                    ROUND(
                        (spend_idr - avg_spend_idr)
                        / NULLIF(stddev_spend_idr, 0)
                    , 2)                                           AS z_score_proxy
                FROM scored
                WHERE spend_idr > avg_spend_idr + (2 * COALESCE(NULLIF(stddev_spend_idr, 0), avg_spend_idr))
                ORDER BY z_score_proxy DESC NULLS LAST, spend_idr DESC
                LIMIT 10
                """,
            ),
        ],
    ),
    (
        "Stakeholder 3 — Direktur Operasional (COO / VP Operations)",
        [
            (
                "OPS1 — Tren Performa Vendor dari Kuartal ke Kuartal",
                """
                WITH vendor_quarter AS (
                    SELECT
                        dd.year_quarter,
                        v.vendor_name,
                        COUNT(*)                                  AS jumlah_line_gr,
                        ROUND(AVG(f.fulfillment_pct), 1)          AS avg_fulfillment_pct,
                        ROUND(
                            100.0 * COUNT(*) FILTER (WHERE f.is_on_time)
                                  / NULLIF(COUNT(*), 0)
                        , 1)                                       AS pct_on_time,
                        ROUND(AVG(f.lead_time_variance_days), 1)  AS avg_variance_days,
                        SUM(f.net_total_ordered_idr)              AS total_neto_idr
                    FROM marts.fct_purchase_order_lines f
                    JOIN marts.dim_date dd
                        ON f.date_key_po_date = dd.date_key
                    JOIN marts.dim_vendors v
                        ON f.vendor_sk = v.vendor_sk
                    WHERE f.is_cancelled = FALSE
                      AND f.has_goods_receipt = TRUE
                    GROUP BY dd.year_quarter, v.vendor_name
                )
                SELECT
                    year_quarter,
                    vendor_name,
                    jumlah_line_gr,
                    avg_fulfillment_pct,
                    pct_on_time,
                    LAG(pct_on_time)
                        OVER (PARTITION BY vendor_name ORDER BY year_quarter)
                                                                    AS prev_q_pct_on_time,
                    avg_variance_days,
                    ROUND(total_neto_idr / 1000000000.0, 2)        AS total_neto_miliar_idr
                FROM vendor_quarter
                ORDER BY year_quarter DESC, total_neto_idr DESC
                LIMIT 15
                """,
            ),
            (
                "OPS2 — Vendor dengan Risiko Ketergantungan Tinggi",
                """
                WITH item_vendor AS (
                    SELECT
                        i.item_category,
                        i.item_name,
                        v.vendor_name,
                        COUNT(*)                                  AS jumlah_line,
                        SUM(f.net_total_ordered_idr)              AS spend_idr
                    FROM marts.fct_purchase_order_lines f
                    JOIN marts.dim_items i
                        ON f.item_sk = i.item_sk
                    JOIN marts.dim_vendors v
                        ON f.vendor_sk = v.vendor_sk
                    WHERE f.is_cancelled = FALSE
                    GROUP BY i.item_category, i.item_name, v.vendor_name
                ),
                ranked AS (
                    SELECT
                        item_category,
                        item_name,
                        vendor_name,
                        jumlah_line,
                        spend_idr,
                        COUNT(*) OVER (PARTITION BY item_category, item_name)
                                                                    AS jumlah_vendor_item,
                        SUM(spend_idr) OVER (PARTITION BY item_category, item_name)
                                                                    AS total_item_spend_idr,
                        ROW_NUMBER() OVER (
                            PARTITION BY item_category, item_name
                            ORDER BY spend_idr DESC
                        )                                           AS vendor_rank
                    FROM item_vendor
                )
                SELECT
                    item_category,
                    item_name,
                    vendor_name                                    AS vendor_dominan,
                    jumlah_vendor_item,
                    jumlah_line,
                    ROUND(spend_idr / 1000000000.0, 2)            AS vendor_spend_miliar_idr,
                    ROUND(
                        100.0 * spend_idr / NULLIF(total_item_spend_idr, 0)
                    , 1)                                           AS pct_spend_item,
                    CASE
                        WHEN jumlah_vendor_item = 1 THEN 'Single Source'
                        WHEN spend_idr / NULLIF(total_item_spend_idr, 0) >= 0.70 THEN 'Risiko Tinggi'
                        WHEN spend_idr / NULLIF(total_item_spend_idr, 0) >= 0.50 THEN 'Risiko Sedang'
                        ELSE 'Tersebar'
                    END                                            AS tingkat_risiko
                FROM ranked
                WHERE vendor_rank = 1
                ORDER BY
                    spend_idr / NULLIF(total_item_spend_idr, 0) DESC NULLS LAST,
                    spend_idr DESC
                LIMIT 10
                """,
            ),
            (
                "OPS3 — Penghematan Negosiasi vs Harga Referensi (Proxy Harga Pasar)",
                """
                SELECT
                    v.vendor_name,
                    i.item_category,
                    COUNT(*)                                      AS jumlah_line,
                    ROUND(SUM(GREATEST(i.current_unit_price_idr - f.net_unit_price_idr, 0)
                              * f.quantity_ordered) / 1000000000.0, 2)
                                                                    AS savings_proxy_miliar_idr,
                    ROUND(SUM(i.current_unit_price_idr * f.quantity_ordered)
                          / 1000000000.0, 2)                      AS nilai_referensi_miliar_idr,
                    ROUND(SUM(f.net_total_ordered_idr)
                          / 1000000000.0, 2)                      AS nilai_negosiasi_miliar_idr,
                    ROUND(
                        100.0 * SUM(GREATEST(i.current_unit_price_idr - f.net_unit_price_idr, 0)
                                    * f.quantity_ordered)
                              / NULLIF(SUM(i.current_unit_price_idr * f.quantity_ordered), 0)
                    , 1)                                           AS pct_savings_proxy
                FROM marts.fct_purchase_order_lines f
                JOIN marts.dim_items i
                    ON f.item_sk = i.item_sk
                JOIN marts.dim_vendors v
                    ON f.vendor_sk = v.vendor_sk
                WHERE f.is_cancelled = FALSE
                GROUP BY v.vendor_name, i.item_category
                ORDER BY SUM(GREATEST(i.current_unit_price_idr - f.net_unit_price_idr, 0)
                             * f.quantity_ordered) DESC
                LIMIT 10
                """,
            ),
            (
                "OPS4 — Departemen Paling Banyak Mengajukan Permintaan Pembelian (Proxy PR)",
                """
                SELECT
                    d.department_name,
                    d.location,
                    COUNT(DISTINCT f.pr_id)                       AS jumlah_pr_proxy,
                    COUNT(DISTINCT f.po_id)                       AS jumlah_po,
                    COUNT(f.po_line_id)                           AS jumlah_line,
                    ROUND(SUM(f.net_total_ordered_idr) / 1000000000.0, 2)
                                                                    AS total_ordered_miliar_idr
                FROM marts.fct_purchase_order_lines f
                JOIN marts.dim_departments d
                    ON f.department_sk = d.department_sk
                WHERE f.is_cancelled = FALSE
                GROUP BY d.department_name, d.location
                ORDER BY COUNT(DISTINCT f.pr_id) DESC, SUM(f.net_total_ordered_idr) DESC
                LIMIT 10
                """,
            ),
        ],
    ),
    (
        "Stakeholder 4 — Staf Logistik / Gudang",
        [
            (
                "WH1 — Kuantitas Barang Diterima per Item per Periode",
                """
                SELECT
                    TO_CHAR(f.gr_date, 'YYYY-MM')                 AS tahun_bulan_gr,
                    i.item_category,
                    i.item_name,
                    f.unit_of_measure,
                    SUM(f.quantity_received_calc)                 AS total_qty_diterima,
                    COUNT(f.po_line_id)                           AS jumlah_line_gr,
                    ROUND(SUM(f.net_total_received_idr) / 1000000000.0, 2)
                                                                    AS nilai_diterima_miliar_idr
                FROM marts.fct_purchase_order_lines f
                JOIN marts.dim_items i
                    ON f.item_sk = i.item_sk
                WHERE f.is_cancelled = FALSE
                  AND f.has_goods_receipt = TRUE
                GROUP BY TO_CHAR(f.gr_date, 'YYYY-MM'), i.item_category, i.item_name, f.unit_of_measure
                ORDER BY tahun_bulan_gr DESC, total_qty_diterima DESC
                LIMIT 15
                """,
            ),
            (
                "WH2 — Ketidaksesuaian Kuantitas PO vs Goods Receipt",
                """
                SELECT
                    i.item_category,
                    i.item_name,
                    f.receipt_status,
                    f.shortage_severity,
                    COUNT(*)                                      AS jumlah_line,
                    SUM(f.quantity_ordered)                       AS total_qty_dipesan,
                    SUM(f.quantity_received_calc)                 AS total_qty_diterima,
                    SUM(f.quantity_variance_gr)                   AS total_variance_gr,
                    ROUND(AVG(f.fulfillment_pct), 1)              AS avg_fulfillment_pct,
                    ROUND(SUM(f.value_shortfall_idr) / 1000000000.0, 2)
                                                                    AS shortfall_miliar_idr
                FROM marts.fct_purchase_order_lines f
                JOIN marts.dim_items i
                    ON f.item_sk = i.item_sk
                WHERE f.is_cancelled = FALSE
                  AND f.has_goods_receipt = TRUE
                  AND f.quantity_variance_gr <> 0
                GROUP BY i.item_category, i.item_name, f.receipt_status, f.shortage_severity
                ORDER BY ABS(SUM(f.quantity_variance_gr)) DESC, SUM(f.value_shortfall_idr) DESC
                LIMIT 15
                """,
            ),
            (
                "WH3 — Item Risiko Kekurangan Stok akibat Keterlambatan (Proxy Stock Shortage)",
                """
                SELECT
                    i.item_category,
                    i.item_name,
                    COUNT(*) FILTER (WHERE f.is_short_delivery)   AS jumlah_short_delivery,
                    COUNT(*) FILTER (WHERE NOT f.is_on_time)      AS jumlah_terlambat,
                    SUM(f.unfulfilled_qty)                        AS total_unfulfilled_qty,
                    ROUND(AVG(f.lead_time_variance_days), 1)      AS avg_delay_days,
                    ROUND(SUM(f.value_shortfall_idr) / 1000000000.0, 2)
                                                                    AS value_shortfall_miliar_idr
                FROM marts.fct_purchase_order_lines f
                JOIN marts.dim_items i
                    ON f.item_sk = i.item_sk
                WHERE f.is_cancelled = FALSE
                  AND f.has_goods_receipt = TRUE
                  AND (f.is_short_delivery = TRUE OR f.is_on_time = FALSE)
                GROUP BY i.item_category, i.item_name
                ORDER BY SUM(f.unfulfilled_qty) DESC, AVG(f.lead_time_variance_days) DESC NULLS LAST
                LIMIT 15
                """,
            ),
        ],
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
    print(f"{'▶'} QUERY BISNIS PER STAKEHOLDER\n")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for stakeholder, queries in BUSINESS_QUERIES:
            print(f"\n  {'◆'} {stakeholder}")
            print("  " + hr("═", 60))
            for title, sql in queries:
                print(f"\n    📊 {title}")
                print("    " + hr("─", 58))
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
