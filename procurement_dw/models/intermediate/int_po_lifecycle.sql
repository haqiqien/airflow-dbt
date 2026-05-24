{{
    config(
        materialized = 'view',
        description  = 'Intermediate: lifecycle lengkap satu PO line dari penerbitan PO hingga penerimaan barang (GR).'
    )
}}

/*
=============================================================================
int_po_lifecycle
=============================================================================
Sumber  : stg_purchase_orders  ×  stg_purchase_order_lines  ×  stg_goods_receipts
Layer   : Intermediate (view)
Grain   : 1 baris per PO Line (line_id)

Tujuan:
  Menjembatani tiga tabel staging menjadi satu dataset kohesif yang siap
  dikonsumsi oleh fct_purchase_order_lines.  Semua metrik turunan dihitung
  di sini agar logika bisnis terpusat dan tidak tersebar di layer marts.

Metrik yang dihitung:
  ┌─────────────────────────────────────────────────────────────────────┐
  │ KUANTITAS                                                           │
  │   unfulfilled_qty       : qty dipesan tapi belum/tidak diterima     │
  │   fulfillment_pct       : % terpenuhi (0–115%)                     │
  │                                                                     │
  │ NILAI (IDR)                                                         │
  │   net_total_ordered_idr : nilai PO setelah diskon                  │
  │   net_total_received_idr: nilai barang diterima × harga diskon     │
  │   value_shortfall_idr   : selisih nilai (ordered – received)       │
  │   discount_amount_idr   : nominal diskon                           │
  │                                                                     │
  │ LEAD TIME (hari)                                                    │
  │   expected_lead_time_days: jarak PO → expected_delivery            │
  │   actual_lead_time_days  : jarak PO → gr_date (NULL jika blm GR)  │
  │   lead_time_variance_days: aktual – ekspektasi (+terlambat)        │
  └─────────────────────────────────────────────────────────────────────┘

Catatan relasi:
  - PO ← PO Lines   : 1 PO memiliki banyak lines
  - PO Lines ← GR   : 1 line memiliki 0 atau 1 GR (telah diverifikasi 1:1)
    Lines tanpa GR   = PO berstatus 'Terbuka' atau 'Dibatalkan'
=============================================================================
*/

with

po as (
    select * from {{ ref('stg_purchase_orders') }}
),

pol as (
    select * from {{ ref('stg_purchase_order_lines') }}
),

gr as (
    -- Defensive DISTINCT ON: jika di masa depan ada multi-GR per line,
    -- ambil GR paling akhir sebagai acuan penerimaan final.
    select distinct on (po_line_id) *
    from {{ ref('stg_goods_receipts') }}
    order by po_line_id, gr_date desc
),

joined as (

    select
        -- ── Kunci Identitas ──────────────────────────────────────────────
        pol.line_id                                            as po_line_id,
        pol.po_id,
        po.pr_id,
        po.vendor_id,
        po.department_id,
        pol.item_id,
        gr.gr_id,

        -- ── Nomor Dokumen ────────────────────────────────────────────────
        po.po_number,
        gr.gr_number,
        pol.line_number,

        -- ── Tanggal (untuk join ke dim_date) ─────────────────────────────
        po.po_date,
        pol.delivery_date_expected,
        gr.gr_date,                             -- NULL jika belum ada GR

        -- Komponen tanggal untuk partisi analitik
        po.po_year,
        po.po_month,
        po.po_year_month,

        -- ── METRIK KUANTITAS ─────────────────────────────────────────────

        pol.quantity_ordered,

        -- qty_received: 0 jika belum ada GR (untuk kalkulasi); tetap NULL
        -- sebagai sinyal "belum diterima" pada kolom asli
        gr.quantity_received,                   -- NULL jika belum GR
        coalesce(gr.quantity_received, 0)        as quantity_received_coalesced,

        -- Selisih dari perspektif GR: positif = lebih dari pesan, negatif = kurang
        coalesce(gr.quantity_variance, 0)        as quantity_variance_gr,

        -- Selisih dari perspektif permintaan: positif = belum terpenuhi
        pol.quantity_ordered
          - coalesce(gr.quantity_received, 0)    as unfulfilled_qty,

        -- % terpenuhi
        case
            when pol.quantity_ordered > 0
            then round(
                   coalesce(gr.quantity_received, 0)::numeric
                   / pol.quantity_ordered::numeric * 100,
                 2)
            else 0
        end                                      as fulfillment_pct,

        -- ── METRIK NILAI (IDR) ────────────────────────────────────────────

        pol.unit_price_idr,
        pol.net_unit_price_idr,
        pol.discount_pct,
        pol.discount_amount_idr,
        pol.has_discount,
        pol.discount_tier,

        -- Nilai pesanan sebelum diskon
        pol.gross_total_price_idr                as gross_total_ordered_idr,

        -- Nilai pesanan setelah diskon (benchmark perbandingan)
        pol.net_total_price_idr                  as net_total_ordered_idr,

        -- Nilai penerimaan aktual: hitung ulang pakai net_unit_price (setelah diskon)
        -- agar apple-to-apple dengan net_total_ordered_idr
        round(
            coalesce(gr.quantity_received, 0)::numeric
            * pol.net_unit_price_idr::numeric,
        0)                                       as net_total_received_idr,

        -- Selisih nilai: berapa Rupiah yang tidak terpenuhi
        pol.net_total_price_idr
          - round(
              coalesce(gr.quantity_received, 0)::numeric
              * pol.net_unit_price_idr::numeric,
            0)                                   as value_shortfall_idr,

        -- Nilai yang ditolak/perlu re-inspeksi (risiko finansial)
        case
            when gr.is_rejected_by_inspection
            then round(
                   coalesce(gr.quantity_received, 0)::numeric
                   * pol.net_unit_price_idr::numeric,
                 0)
            else 0
        end                                      as rejected_goods_value_idr,

        -- ── METRIK LEAD TIME (hari) ───────────────────────────────────────

        -- Lead time yang dijanjikan saat PO terbit
        po.expected_lead_time_days,

        -- Lead time aktual (NULL jika belum ada GR)
        gr.lead_time_days                        as actual_lead_time_days,

        -- Variansi: positif = terlambat, negatif = lebih cepat dari janji
        case
            when gr.lead_time_days is not null
            then gr.lead_time_days - po.expected_lead_time_days
            else null
        end                                      as lead_time_variance_days,

        -- ── STATUS & LIFECYCLE ────────────────────────────────────────────

        po.po_status,
        pol.line_status,

        -- Tahap lifecycle order
        case
            when po.is_cancelled
                then 'Dibatalkan'
            when gr.gr_id is null and po.is_open
                then 'Menunggu Penerimaan'
            when gr.gr_id is null and not po.is_open
                then 'Ditutup Tanpa Penerimaan'
            when gr.quantity_received >= pol.quantity_ordered
                then 'Selesai – Terpenuhi Penuh'
            when gr.quantity_received > 0
                then 'Selesai – Terpenuhi Sebagian'
            else
                'Tidak Ada Penerimaan'
        end                                      as lifecycle_stage,

        gr.receipt_status,
        gr.delivery_timeliness,
        gr.shortage_severity,
        gr.is_on_time,
        gr.is_short_delivery,
        gr.is_over_delivery,
        gr.is_full_delivery,

        -- ── FLAGS ─────────────────────────────────────────────────────────

        -- Apakah GR sudah ada
        case when gr.gr_id is not null then true else false end
                                                 as has_goods_receipt,

        -- Apakah terpenuhi sepenuhnya
        case
            when gr.quantity_received >= pol.quantity_ordered then true
            else false
        end                                      as is_fully_fulfilled,

        -- Inspeksi
        coalesce(gr.is_rejected_by_inspection, false)
                                                 as is_rejected_by_inspection,
        coalesce(gr.needs_reinspection, false)   as needs_reinspection,

        -- ── ATRIBUT TAMBAHAN ──────────────────────────────────────────────

        pol.unit_of_measure,
        gr.warehouse_location,
        gr.inspection_status,
        po.payment_terms,
        po.is_cancelled,
        po.is_completed

    from pol
    inner join po
        on pol.po_id = po.po_id
    left join gr
        on pol.line_id = gr.po_line_id
)

select * from joined
