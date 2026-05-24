{{
    config(
        materialized = 'table',
        description  = 'Fakta Accumulating Snapshot: satu baris per PO line, terakumulasi dari PO → expected delivery → GR.'
    )
}}

/*
=============================================================================
fct_purchase_order_lines  —  Accumulating Snapshot Fact Table
=============================================================================
Layer   : Marts – Fakta (table)
Grain   : 1 baris per PO Line (po_line_id / line_id)

POLA: ACCUMULATING SNAPSHOT
  Berbeda dengan Transaction fact (1 event = 1 baris baru), Accumulating
  Snapshot memperbarui (UPDATE) baris yang sama saat setiap milestone
  lifecycle terjadi.

  Milestone yang dilacak:
    ┌──────────┬────────────────────────────────┬──────────────────────────┐
    │ Milestone│ Kolom tanggal                  │ Kolom status             │
    ├──────────┼────────────────────────────────┼──────────────────────────┤
    │ M1 – PO  │ po_date (date_key_po_date)     │ po_status                │
    │ M2 – EXP │ delivery_date_expected         │ expected_lead_time_days  │
    │           │ (date_key_delivery_expected)  │                          │
    │ M3 – GR  │ gr_date (date_key_gr_date)     │ receipt_status,          │
    │           │ NULL = belum diterima          │ actual_lead_time_days    │
    └──────────┴────────────────────────────────┴──────────────────────────┘

SURROGATE KEY MAPPING:
  ┌────────────────────────┬────────────────────┬──────────────────────────┐
  │ FK di Fakta            │ Dimensi target     │ Keterangan               │
  ├────────────────────────┼────────────────────┼──────────────────────────┤
  │ date_key_po_date       │ dim_date.date_key  │ Tanggal PO diterbitkan   │
  │ date_key_delivery_exp  │ dim_date.date_key  │ Target pengiriman        │
  │ date_key_gr_date       │ dim_date.date_key  │ Penerimaan aktual / NULL │
  │ vendor_sk              │ dim_vendors        │ Current record SCD2      │
  │ department_sk          │ dim_departments    │ Static SCD0              │
  │ item_sk                │ dim_items          │ SCD Type 1               │
  └────────────────────────┴────────────────────┴──────────────────────────┘

MEASURES:
  Kuantitas : quantity_ordered, quantity_received, unfulfilled_qty,
              quantity_variance_gr, fulfillment_pct
  Nilai IDR : unit_price_idr, net_unit_price_idr, discount_pct,
              gross_total_ordered_idr, net_total_ordered_idr,
              net_total_received_idr, value_shortfall_idr,
              discount_amount_idr, rejected_goods_value_idr
  Lead Time : expected_lead_time_days, actual_lead_time_days,
              lead_time_variance_days
=============================================================================
*/

with

lifecycle as (
    select * from {{ ref('int_po_lifecycle') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
),

dim_vendors as (
    -- Join ke current record SCD Type 2 agar tidak terjadi fan-out
    -- (satu vendor_id hanya punya satu current record)
    select vendor_sk, vendor_id
    from {{ ref('dim_vendors') }}
    where is_current_record = true
),

dim_departments as (
    select department_sk, department_id
    from {{ ref('dim_departments') }}
),

dim_items as (
    select item_sk, item_id
    from {{ ref('dim_items') }}
),

final as (

    select

        -- ══════════════════════════════════════════════════════════════════
        -- SURROGATE KEY FAKTA
        -- ══════════════════════════════════════════════════════════════════
        md5(lc.po_line_id::text)                       as fct_po_line_sk,

        -- ══════════════════════════════════════════════════════════════════
        -- FOREIGN KEYS KE DIMENSI
        -- ══════════════════════════════════════════════════════════════════

        -- Tanggal M1: PO diterbitkan
        d_po.date_key                                  as date_key_po_date,

        -- Tanggal M2: target pengiriman yang dijanjikan
        d_exp.date_key                                 as date_key_delivery_expected,

        -- Tanggal M3: penerimaan aktual (NULL = milestone belum tercapai)
        d_gr.date_key                                  as date_key_gr_date,

        -- Dimensi lain
        dv.vendor_sk,
        dd.department_sk,
        di.item_sk,

        -- ══════════════════════════════════════════════════════════════════
        -- NATURAL KEYS (untuk debugging, tracing, dan row-level drill-through)
        -- ══════════════════════════════════════════════════════════════════
        lc.po_line_id,
        lc.po_id,
        lc.pr_id,
        lc.vendor_id,
        lc.department_id,
        lc.item_id,
        lc.gr_id,                                      -- NULL jika belum ada GR

        -- ══════════════════════════════════════════════════════════════════
        -- TANGGAL AKTUAL (untuk kalkulasi ad-hoc tanpa join ke dim_date)
        -- ══════════════════════════════════════════════════════════════════
        lc.po_date,
        lc.po_year,
        lc.po_month,
        lc.po_year_month,
        lc.delivery_date_expected,
        lc.gr_date,                                    -- NULL jika belum GR

        -- ══════════════════════════════════════════════════════════════════
        -- MEASURES: KUANTITAS
        -- ══════════════════════════════════════════════════════════════════
        lc.quantity_ordered,

        -- quantity_received: NULL menandakan belum ada GR (lebih informatif
        -- daripada 0, agar bisa dibedakan antara "belum diterima" vs "0 diterima")
        lc.quantity_received,

        -- Gunakan coalesced version untuk kalkulasi numerik
        lc.quantity_received_coalesced             as quantity_received_calc,

        -- Selisih dari perspektif GR (+lebih, -kurang)
        lc.quantity_variance_gr,

        -- Kuantitas yang belum/tidak terpenuhi (demand shortfall)
        lc.unfulfilled_qty,

        -- Persentase pemenuhan (0 = tidak ada GR, 100 = terpenuhi penuh)
        lc.fulfillment_pct,

        -- ══════════════════════════════════════════════════════════════════
        -- MEASURES: NILAI (IDR)
        -- ══════════════════════════════════════════════════════════════════

        -- Harga satuan
        lc.unit_price_idr,                             -- sebelum diskon
        lc.net_unit_price_idr,                         -- setelah diskon
        lc.discount_pct,
        lc.has_discount,
        lc.discount_tier,
        lc.discount_amount_idr,                        -- nominal diskon (IDR)

        -- Nilai pesanan
        lc.gross_total_ordered_idr,                    -- qty × unit_price (sebelum diskon)
        lc.net_total_ordered_idr,                      -- qty × net_unit_price (setelah diskon)

        -- Nilai penerimaan aktual (dihitung ulang dengan net_unit_price)
        lc.net_total_received_idr,

        -- Selisih nilai: berapa Rupiah yang tidak terpenuhi
        lc.value_shortfall_idr,

        -- Nilai barang yang ditolak inspeksi (risiko finansial)
        lc.rejected_goods_value_idr,

        -- ══════════════════════════════════════════════════════════════════
        -- MEASURES: LEAD TIME (hari)
        -- ══════════════════════════════════════════════════════════════════
        lc.expected_lead_time_days,
        lc.actual_lead_time_days,                      -- NULL jika belum GR

        -- Variansi lead time: positif = terlambat, negatif = lebih cepat
        lc.lead_time_variance_days,                    -- NULL jika belum GR

        -- ══════════════════════════════════════════════════════════════════
        -- STATUS & LIFECYCLE
        -- ══════════════════════════════════════════════════════════════════
        lc.po_status,
        lc.line_status,
        lc.lifecycle_stage,
        lc.receipt_status,
        lc.delivery_timeliness,
        lc.shortage_severity,

        -- ══════════════════════════════════════════════════════════════════
        -- FLAGS (untuk filter cepat tanpa CASE WHEN di query analitik)
        -- ══════════════════════════════════════════════════════════════════
        lc.has_goods_receipt,           -- milestone M3 sudah tercapai
        lc.is_fully_fulfilled,          -- qty_received >= qty_ordered
        lc.is_short_delivery,           -- qty_received < qty_ordered
        lc.is_over_delivery,            -- qty_received > qty_ordered
        lc.is_full_delivery,            -- qty_received = qty_ordered
        lc.is_on_time,                  -- gr_date <= delivery_date_expected
        lc.is_cancelled,
        lc.is_completed,
        lc.is_rejected_by_inspection,
        lc.needs_reinspection,

        -- ══════════════════════════════════════════════════════════════════
        -- ATRIBUT DEGENERATE (dari dokumen transaksi, bukan dari dimensi)
        -- Disimpan di fakta untuk menghindari join tambahan di query sederhana
        -- ══════════════════════════════════════════════════════════════════
        lc.line_number,
        lc.unit_of_measure,
        lc.warehouse_location,
        lc.inspection_status,
        lc.payment_terms,
        lc.po_number,
        lc.gr_number

    from lifecycle lc

    -- ── Join ke dim_date (M1: PO date) ───────────────────────────────────
    left join dim_date d_po
        on lc.po_date = d_po.full_date

    -- ── Join ke dim_date (M2: expected delivery) ─────────────────────────
    left join dim_date d_exp
        on lc.delivery_date_expected = d_exp.full_date

    -- ── Join ke dim_date (M3: GR date) ────────────────────────────────────
    left join dim_date d_gr
        on lc.gr_date = d_gr.full_date

    -- ── Join ke dim_vendors (current record SCD Type 2) ───────────────────
    left join dim_vendors dv
        on lc.vendor_id = dv.vendor_id

    -- ── Join ke dim_departments ───────────────────────────────────────────
    left join dim_departments dd
        on lc.department_id = dd.department_id

    -- ── Join ke dim_items ─────────────────────────────────────────────────
    left join dim_items di
        on lc.item_id = di.item_id

)

select * from final
