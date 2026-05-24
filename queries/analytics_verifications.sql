/*
=============================================================================
analytics_verifications.sql
=============================================================================
Tujuan : Memverifikasi kualitas dan kelengkapan data mart sekaligus
         menjawab 5 pertanyaan bisnis utama dari domain Procurement.

Skema  : marts  (atau sesuaikan dengan nama schema di environment Anda)
Tabel  : marts.fct_purchase_order_lines   — fakta utama (Accumulating Snapshot)
         marts.dim_vendors                — dimensi vendor (SCD Type 2)
         marts.dim_departments            — dimensi departemen (SCD Type 0)
         marts.dim_items                  — dimensi item (SCD Type 1)
         marts.dim_date                   — dimensi tanggal

Catatan eksekusi:
  • Jalankan setiap kueri secara terpisah, atau ganti pembatas `;` dengan
    pemisah yang sesuai dengan tool/BI yang Anda gunakan.
  • Semua join ke dim_vendors menggunakan vendor_sk (surrogate key current
    record) yang sudah difilter is_current_record = TRUE di fakta.
  • Kolom quantity_received / gr_date bisa NULL (PO line belum diterima).
=============================================================================
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 1 — Total Nilai Purchase Order (PO) per Bulan
-- ─────────────────────────────────────────────────────────────────────────────
-- Pertanyaan: Berapa total nilai neto PO yang diterbitkan setiap bulan?
--
-- Logika:
--   • Grain PO line → aggregate ke bulan menggunakan po_year_month
--     (kolom denormalisasi di fakta, format 'YYYY-MM').
--   • Gunakan net_total_ordered_idr (setelah diskon) sebagai nilai resmi.
--   • Hitung juga jumlah PO unik & jumlah line untuk konteks volume.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    f.po_year                                                      AS tahun,
    f.po_month                                                     AS bulan,
    f.po_year_month                                                AS tahun_bulan,

    COUNT(DISTINCT f.po_id)                                        AS jumlah_po,
    COUNT(f.po_line_id)                                            AS jumlah_po_line,

    -- Nilai pesanan neto (setelah diskon)
    SUM(f.net_total_ordered_idr)                                   AS total_nilai_neto_idr,
    ROUND(AVG(f.net_total_ordered_idr), 0)                         AS rata_rata_nilai_per_line_idr,

    -- Nilai bruto untuk perbandingan
    SUM(f.gross_total_ordered_idr)                                 AS total_nilai_bruto_idr,

    -- Total diskon yang diberikan bulan ini
    SUM(f.discount_amount_idr)                                     AS total_diskon_idr,
    ROUND(
        100.0 * SUM(f.discount_amount_idr)
              / NULLIF(SUM(f.gross_total_ordered_idr), 0)
    , 2)                                                           AS pct_diskon_rata_rata,

    -- Perubahan MoM (Month-over-Month) — gunakan LAG di lapisan BI/aplikasi
    -- karena PostgreSQL memerlukan CTE terpisah untuk LAG
    ROUND(
        SUM(f.net_total_ordered_idr) / 1000000.0
    , 2)                                                           AS total_nilai_neto_juta_idr

FROM marts.fct_purchase_order_lines f
WHERE
    f.is_cancelled = FALSE                   -- tidak sertakan PO yang dibatalkan
GROUP BY
    f.po_year,
    f.po_month,
    f.po_year_month
ORDER BY
    f.po_year_month;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 2 — Top 5 Vendor dengan Volume Transaksi Terbesar
-- ─────────────────────────────────────────────────────────────────────────────
-- Pertanyaan: Vendor mana saja (Top 5) yang memiliki volume transaksi terbesar?
--
-- Logika:
--   • "Volume transaksi" = total nilai neto PO line yang diterbitkan.
--   • Join ke dim_vendors untuk mendapatkan nama & kategori vendor.
--   • Filter is_current_record = TRUE sudah diterapkan di fakta saat
--     build time (JOIN hanya ke current record); tidak perlu filter ulang.
--   • Tampilkan juga metrik kualitas: rata-rata fulfillment, keterlambatan.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    v.vendor_id,
    v.vendor_name,
    v.vendor_category,
    v.vendor_type,
    v.city                                                         AS kota,
    v.province                                                     AS provinsi,
    v.payment_terms                                                AS syarat_pembayaran,

    -- Volume & Nilai
    COUNT(DISTINCT f.po_id)                                        AS jumlah_po,
    COUNT(f.po_line_id)                                            AS jumlah_po_line,
    SUM(f.net_total_ordered_idr)                                   AS total_nilai_neto_idr,
    ROUND(SUM(f.net_total_ordered_idr) / 1000000.0, 2)            AS total_nilai_neto_juta,

    -- Metrik Kualitas Penerimaan
    ROUND(AVG(f.fulfillment_pct), 1)                               AS avg_fulfillment_pct,
    SUM(CASE WHEN f.has_goods_receipt THEN 1 ELSE 0 END)           AS jumlah_line_ada_gr,
    SUM(CASE WHEN f.is_fully_fulfilled THEN 1 ELSE 0 END)          AS jumlah_line_terpenuhi,

    -- Metrik Ketepatan Waktu
    SUM(CASE WHEN f.is_on_time AND f.has_goods_receipt THEN 1 ELSE 0 END)
                                                                   AS jumlah_on_time,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_on_time AND f.has_goods_receipt THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN f.has_goods_receipt THEN 1 ELSE 0 END), 0)
    , 1)                                                           AS pct_on_time_delivery,

    -- Ranking berdasarkan total nilai
    RANK() OVER (ORDER BY SUM(f.net_total_ordered_idr) DESC)       AS ranking_nilai

FROM marts.fct_purchase_order_lines f
JOIN marts.dim_vendors v
    ON f.vendor_sk = v.vendor_sk
WHERE
    f.is_cancelled = FALSE
GROUP BY
    v.vendor_id,
    v.vendor_name,
    v.vendor_category,
    v.vendor_type,
    v.city,
    v.province,
    v.payment_terms
ORDER BY
    total_nilai_neto_idr DESC
LIMIT 5;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 3 — Rata-rata Lead Time per Kategori Item
-- ─────────────────────────────────────────────────────────────────────────────
-- Pertanyaan: Berapa rata-rata waktu siklus (lead time) dari penerbitan PO
--             hingga penerimaan barang per kategori item?
--
-- Logika:
--   • Hanya baris dengan has_goods_receipt = TRUE yang memiliki
--     actual_lead_time_days (non-NULL).
--   • Bandingkan: expected_lead_time (dari PO) vs actual_lead_time (dari GR).
--   • lead_time_variance_days > 0 = terlambat; < 0 = lebih cepat dari target.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    i.item_category                                                AS kategori_item,

    -- Volume
    COUNT(f.po_line_id)                                            AS total_po_line,
    SUM(CASE WHEN f.has_goods_receipt THEN 1 ELSE 0 END)           AS po_line_ada_gr,

    -- Lead Time yang Dijanjikan (dari PO, semua baris)
    ROUND(AVG(f.expected_lead_time_days), 1)                       AS rata_expected_lead_time_hari,
    MIN(f.expected_lead_time_days)                                 AS min_expected_lead_time,
    MAX(f.expected_lead_time_days)                                 AS max_expected_lead_time,

    -- Lead Time Aktual (hanya baris yang sudah ada GR)
    ROUND(AVG(f.actual_lead_time_days), 1)                         AS rata_actual_lead_time_hari,
    MIN(f.actual_lead_time_days)                                   AS min_actual_lead_time,
    MAX(f.actual_lead_time_days)                                   AS max_actual_lead_time,

    -- Variansi: positif = terlambat, negatif = lebih cepat
    ROUND(AVG(f.lead_time_variance_days), 1)                       AS rata_variansi_lead_time_hari,

    -- Persentase pengiriman tepat waktu per kategori
    ROUND(
        100.0 * SUM(CASE WHEN f.is_on_time AND f.has_goods_receipt THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN f.has_goods_receipt THEN 1 ELSE 0 END), 0)
    , 1)                                                           AS pct_on_time,

    -- Distribusi keterlambatan
    SUM(CASE WHEN f.delivery_timeliness = 'Tepat Waktu'     THEN 1 ELSE 0 END) AS tepat_waktu,
    SUM(CASE WHEN f.delivery_timeliness = 'Terlambat Ringan' THEN 1 ELSE 0 END) AS terlambat_ringan,
    SUM(CASE WHEN f.delivery_timeliness = 'Terlambat Sedang' THEN 1 ELSE 0 END) AS terlambat_sedang,
    SUM(CASE WHEN f.delivery_timeliness = 'Terlambat Parah'  THEN 1 ELSE 0 END) AS terlambat_parah

FROM marts.fct_purchase_order_lines f
JOIN marts.dim_items i
    ON f.item_sk = i.item_sk
WHERE
    f.is_cancelled = FALSE
GROUP BY
    i.item_category
ORDER BY
    rata_actual_lead_time_hari DESC NULLS LAST;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 4 — Ketidaksesuaian Kuantitas Dipesan vs Diterima
-- ─────────────────────────────────────────────────────────────────────────────
-- Pertanyaan: Apakah ada ketidaksesuaian (selisih) antara kuantitas yang
--             dipesan dan diterima?
--
-- Logika:
--   • Ringkasan per tingkat keparahan kekurangan (shortage_severity).
--   • Pisahkan: belum ada GR, diterima penuh, kurang, kelebihan.
--   • Hitung dampak finansial: value_shortfall_idr.
--   • Sertakan analisis per tahun untuk melihat tren.
-- ─────────────────────────────────────────────────────────────────────────────

-- 4a. Ringkasan Kesesuaian Kuantitas (Keseluruhan)
SELECT
    '=== RINGKASAN KETIDAKSESUAIAN KUANTITAS ===' AS laporan,
    NULL::text AS detail
UNION ALL
SELECT
    'Total PO Lines',
    COUNT(*)::text
FROM marts.fct_purchase_order_lines WHERE is_cancelled = FALSE
UNION ALL
SELECT
    'Belum Ada Penerimaan (GR)',
    COUNT(*)::text
FROM marts.fct_purchase_order_lines WHERE is_cancelled = FALSE AND has_goods_receipt = FALSE
UNION ALL
SELECT
    'Penerimaan Lengkap (100%)',
    COUNT(*)::text
FROM marts.fct_purchase_order_lines WHERE is_cancelled = FALSE AND is_fully_fulfilled = TRUE
UNION ALL
SELECT
    'Penerimaan Kurang (< 100%)',
    COUNT(*)::text
FROM marts.fct_purchase_order_lines WHERE is_cancelled = FALSE AND is_short_delivery = TRUE
UNION ALL
SELECT
    'Penerimaan Lebih (> 100%)',
    COUNT(*)::text
FROM marts.fct_purchase_order_lines WHERE is_cancelled = FALSE AND is_over_delivery = TRUE;


-- 4b. Detail Distribusi Ketidaksesuaian per Tingkat Keparahan
SELECT
    f.shortage_severity                                            AS tingkat_kekurangan,
    COUNT(f.po_line_id)                                            AS jumlah_po_line,
    ROUND(
        100.0 * COUNT(f.po_line_id)
              / SUM(COUNT(f.po_line_id)) OVER ()
    , 1)                                                           AS pct_dari_total,

    -- Kuantitas
    SUM(f.quantity_ordered)                                        AS total_qty_dipesan,
    SUM(f.quantity_received_calc)                                  AS total_qty_diterima,
    SUM(f.unfulfilled_qty)                                         AS total_qty_tidak_terpenuhi,

    -- Nilai Finansial
    SUM(f.net_total_ordered_idr)                                   AS total_nilai_dipesan_idr,
    SUM(f.net_total_received_idr)                                  AS total_nilai_diterima_idr,
    SUM(f.value_shortfall_idr)                                     AS total_nilai_shortfall_idr,

    -- Rata-rata persentase pemenuhan per kelompok
    ROUND(AVG(f.fulfillment_pct), 1)                               AS avg_fulfillment_pct,

    -- Dampak barang ditolak inspeksi
    SUM(f.rejected_goods_value_idr)                                AS total_nilai_barang_ditolak_idr,
    SUM(CASE WHEN f.is_rejected_by_inspection THEN 1 ELSE 0 END)   AS jumlah_line_ada_penolakan

FROM marts.fct_purchase_order_lines f
WHERE
    f.is_cancelled = FALSE
GROUP BY
    f.shortage_severity
ORDER BY
    -- Urutkan: Tidak Ada GR → Lengkap → Kurang (ringan ke parah) → Lebih
    CASE f.shortage_severity
        WHEN 'Tidak Ada GR'        THEN 1
        WHEN 'Terpenuhi Penuh'     THEN 2
        WHEN 'Kekurangan Ringan'   THEN 3
        WHEN 'Kekurangan Sedang'   THEN 4
        WHEN 'Kekurangan Parah'    THEN 5
        ELSE 6
    END;


-- 4c. Tren Ketidaksesuaian per Tahun (untuk melihat apakah membaik/memburuk)
SELECT
    f.po_year                                                      AS tahun,
    COUNT(f.po_line_id)                                            AS total_po_line,
    SUM(CASE WHEN f.is_fully_fulfilled  THEN 1 ELSE 0 END)         AS terpenuhi_penuh,
    SUM(CASE WHEN f.is_short_delivery   THEN 1 ELSE 0 END)         AS kekurangan,
    SUM(CASE WHEN f.is_over_delivery    THEN 1 ELSE 0 END)         AS kelebihan,
    SUM(CASE WHEN NOT f.has_goods_receipt THEN 1 ELSE 0 END)       AS belum_ada_gr,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_fully_fulfilled THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN f.has_goods_receipt THEN 1 ELSE 0 END), 0)
    , 1)                                                           AS pct_terpenuhi_penuh,
    ROUND(AVG(CASE WHEN f.has_goods_receipt THEN f.fulfillment_pct END), 1)
                                                                   AS avg_fulfillment_pct

FROM marts.fct_purchase_order_lines f
WHERE
    f.is_cancelled = FALSE
GROUP BY
    f.po_year
ORDER BY
    f.po_year;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 5 — Total Pengeluaran per Departemen
-- ─────────────────────────────────────────────────────────────────────────────
-- Pertanyaan: Berapa total pengeluaran per departemen?
--
-- Logika:
--   • "Pengeluaran" yang terealisasi = net_total_received_idr
--     (hanya nilai yang benar-benar diterima dan tidak ditolak inspeksi).
--   • Bandingkan dengan nilai yang dipesan (net_total_ordered_idr) untuk
--     mengukur realisasi anggaran.
--   • Tampilkan % utilisasi anggaran tahunan jika tersedia di dim_departments.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    d.department_id,
    d.department_name                                              AS nama_departemen,
    d.cost_center                                                  AS kode_cost_center,
    d.location                                                     AS lokasi,
    d.manager_name                                                 AS manajer,
    d.budget_tier                                                  AS segmen_anggaran,
    ROUND(d.budget_annual_idr / 1000000.0, 1)                      AS anggaran_tahunan_juta_idr,

    -- Volume Pembelian
    COUNT(DISTINCT f.po_id)                                        AS jumlah_po,
    COUNT(f.po_line_id)                                            AS jumlah_po_line,

    -- Nilai Pesanan (komitmen)
    ROUND(SUM(f.net_total_ordered_idr) / 1000000.0, 2)            AS total_dipesan_juta_idr,

    -- Nilai Realisasi (barang sudah diterima)
    ROUND(SUM(f.net_total_received_idr) / 1000000.0, 2)           AS total_diterima_juta_idr,

    -- Shortfall: nilai yang dipesan tapi belum/tidak diterima
    ROUND(SUM(f.value_shortfall_idr) / 1000000.0, 2)              AS total_shortfall_juta_idr,

    -- Nilai barang ditolak inspeksi (risiko kualitas finansial)
    ROUND(SUM(f.rejected_goods_value_idr) / 1000000.0, 2)         AS nilai_barang_ditolak_juta_idr,

    -- % Realisasi terhadap Pesanan
    ROUND(
        100.0 * SUM(f.net_total_received_idr)
              / NULLIF(SUM(f.net_total_ordered_idr), 0)
    , 1)                                                           AS pct_realisasi_pesanan,

    -- % Utilisasi Anggaran Tahunan (total pesanan vs anggaran departemen)
    -- Catatan: anggaran departemen adalah tahunan; data mungkin multi-tahun.
    -- Ini adalah proxy — untuk akurasi gunakan filter by year.
    ROUND(
        100.0 * SUM(f.net_total_ordered_idr)
              / NULLIF(d.budget_annual_idr, 0)
    , 1)                                                           AS pct_utilisasi_anggaran_kumulatif,

    -- Distribusi status PO
    SUM(CASE WHEN f.is_completed  THEN 1 ELSE 0 END)               AS po_line_selesai,
    SUM(CASE WHEN f.is_cancelled  THEN 1 ELSE 0 END)               AS po_line_dibatalkan,
    SUM(CASE WHEN NOT f.has_goods_receipt AND NOT f.is_cancelled AND NOT f.is_completed
             THEN 1 ELSE 0 END)                                    AS po_line_dalam_proses,

    -- Ranking departemen berdasarkan total realisasi pengeluaran
    RANK() OVER (ORDER BY SUM(f.net_total_received_idr) DESC)      AS ranking_pengeluaran

FROM marts.fct_purchase_order_lines f
JOIN marts.dim_departments d
    ON f.department_sk = d.department_sk
GROUP BY
    d.department_id,
    d.department_name,
    d.cost_center,
    d.location,
    d.manager_name,
    d.budget_tier,
    d.budget_annual_idr
ORDER BY
    SUM(f.net_total_received_idr) DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY BONUS — Validasi Row Count & Konsistensi Antar Tabel
-- ─────────────────────────────────────────────────────────────────────────────
-- Berguna sebagai smoke test setelah dbt run untuk memastikan
-- semua tabel terisi dengan volume yang diharapkan.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    'dim_date'                                  AS nama_tabel,
    COUNT(*)                                    AS jumlah_baris,
    '3652 (2018-2027, 10 tahun)'                AS ekspektasi
FROM marts.dim_date

UNION ALL

SELECT
    'dim_departments',
    COUNT(*),
    '30 departemen'
FROM marts.dim_departments

UNION ALL

SELECT
    'dim_items',
    COUNT(*),
    '300 item'
FROM marts.dim_items

UNION ALL

SELECT
    'dim_vendors (current only)',
    COUNT(*),
    '150 (satu per vendor_id)'
FROM marts.dim_vendors
WHERE is_current_record = TRUE

UNION ALL

SELECT
    'dim_vendors (all versions)',
    COUNT(*),
    '~169 (150 current + ~19 historical)'
FROM marts.dim_vendors

UNION ALL

SELECT
    'fct_purchase_order_lines',
    COUNT(*),
    '~10821 PO lines'
FROM marts.fct_purchase_order_lines

UNION ALL

SELECT
    'fct — has_goods_receipt = TRUE',
    COUNT(*),
    '~8174 (sesuai jumlah GR lines)'
FROM marts.fct_purchase_order_lines
WHERE has_goods_receipt = TRUE

UNION ALL

SELECT
    'fct — vendor_sk NULL (orphan)',
    COUNT(*),
    '0 (tidak boleh ada orphan vendor)'
FROM marts.fct_purchase_order_lines
WHERE vendor_sk IS NULL

UNION ALL

SELECT
    'fct — date_key_po_date NULL (orphan)',
    COUNT(*),
    '0 (tidak boleh ada orphan date PO)'
FROM marts.fct_purchase_order_lines
WHERE date_key_po_date IS NULL

ORDER BY
    nama_tabel;
