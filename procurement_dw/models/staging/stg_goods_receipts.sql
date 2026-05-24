{{
    config(
        materialized = 'view',
        description  = 'Staging: penerimaan barang (GR). Skenario analitik lead time dan variasi kuantitas siap digunakan di marts.'
    )
}}

/*
=============================================================================
stg_goods_receipts
=============================================================================
Sumber  : raw.goods_receipts
Layer   : Staging (view)
Tujuan  :
  - TRIM semua kolom TEXT
  - unit_price / total_received_value (campur BIGINT + DOUBLE PRECISION) → NUMERIC
  - variance_pct (DOUBLE PRECISION) → NUMERIC(7,2)
  - Cast _ingested_at (TEXT) → TIMESTAMP
  - Tambah kolom turunan analitik:
      delivery_timeliness   : 'Tepat Waktu' / 'Terlambat' + kategori keterlambatan
      quantity_fulfillment_pct : persentase kuantitas yang terpenuhi (0–115%)
      is_short_delivery     : TRUE jika ada kekurangan penerimaan
      is_over_delivery      : TRUE jika ada kelebihan penerimaan
      is_rejected           : TRUE jika inspection_status = 'Ditolak'
      gr_year / gr_month    : komponen tanggal
=============================================================================
*/

with

source as (

    select * from {{ source('raw_procurement', 'goods_receipts') }}

),

cleaned as (

    select
        -- ── Primary Key ───────────────────────────────────────────────────
        gr_id                                                  as gr_id,

        -- ── Nomor & Tanggal ───────────────────────────────────────────────
        trim(gr_number)                                        as gr_number,
        gr_date                                                as gr_date,

        -- Komponen tanggal untuk analisis tren penerimaan
        extract(year  from gr_date)::int                       as gr_year,
        extract(month from gr_date)::int                       as gr_month,
        to_char(gr_date, 'YYYY-MM')                            as gr_year_month,

        -- ── Foreign Keys ──────────────────────────────────────────────────
        po_id                                                  as po_id,
        po_line_id                                             as po_line_id,
        item_id                                                as item_id,

        -- ── Kuantitas ─────────────────────────────────────────────────────
        quantity_ordered                                       as quantity_ordered,
        quantity_received                                      as quantity_received,
        quantity_variance                                      as quantity_variance,

        -- Persentase variance; NUMERIC(7,2) untuk dua desimal yang presisi
        round(variance_pct::numeric, 2)                        as variance_pct,

        -- Persentase kuantitas yang terpenuhi (100% = lengkap)
        -- Dibutuhkan untuk analisis fill rate / service level vendor
        case
            when quantity_ordered > 0
            then round((quantity_received::numeric / quantity_ordered::numeric) * 100, 2)
            else null
        end                                                    as quantity_fulfillment_pct,

        -- ── Status Penerimaan ─────────────────────────────────────────────
        trim(receipt_status)                                   as receipt_status,

        -- ── Lead Time ─────────────────────────────────────────────────────
        -- Skenario analitik utama: berapa hari dari penerbitan PO ke penerimaan aktual
        lead_time_days                                         as lead_time_days,
        is_on_time                                             as is_on_time,

        -- Kategorisasi keterlambatan untuk analitik yang lebih kaya
        case
            when is_on_time = true
                then 'Tepat Waktu'
            when lead_time_days <= 7
                then 'Terlambat ≤1 Minggu'
            when lead_time_days <= 14
                then 'Terlambat ≤2 Minggu'
            when lead_time_days <= 30
                then 'Terlambat ≤1 Bulan'
            else
                'Terlambat >1 Bulan'
        end                                                    as delivery_timeliness,

        -- ── Kolom Turunan: Flags ──────────────────────────────────────────
        -- Memudahkan filter / agregasi tanpa CASE WHEN berulang di marts
        case
            when quantity_variance < 0 then true else false
        end                                                    as is_short_delivery,

        case
            when quantity_variance > 0 then true else false
        end                                                    as is_over_delivery,

        case
            when quantity_variance = 0 then true else false
        end                                                    as is_full_delivery,

        -- Tingkat keparahan kekurangan (untuk prioritas penanganan)
        case
            when quantity_variance >= 0
                then 'Tidak Ada Kekurangan'
            when abs(quantity_variance)::numeric / nullif(quantity_ordered, 0)::numeric <= 0.10
                then 'Kurang Ringan (<10%)'
            when abs(quantity_variance)::numeric / nullif(quantity_ordered, 0)::numeric <= 0.25
                then 'Kurang Sedang (10–25%)'
            else
                'Kurang Parah (>25%)'
        end                                                    as shortage_severity,

        -- ── Finansial ─────────────────────────────────────────────────────
        -- unit_price di raw bertype DOUBLE PRECISION; normalkan ke NUMERIC
        coalesce(unit_price, 0)::numeric                       as unit_price_idr,
        coalesce(total_received_value, 0)::numeric             as total_received_value_idr,

        -- Nilai barang yang TIDAK diterima (potensi klaim ke vendor)
        case
            when quantity_variance < 0
            then round(abs(quantity_variance)::numeric * coalesce(unit_price, 0)::numeric, 0)
            else 0
        end                                                    as shortage_value_idr,

        -- ── Gudang & Inspeksi ─────────────────────────────────────────────
        trim(warehouse_location)                               as warehouse_location,
        trim(received_by)                                      as received_by,
        trim(inspection_status)                                as inspection_status,

        case when trim(inspection_status) = 'Ditolak' then true else false end
                                                               as is_rejected_by_inspection,
        case when trim(inspection_status) = 'Perlu Pemeriksaan Ulang' then true else false end
                                                               as needs_reinspection,

        -- ── Catatan ───────────────────────────────────────────────────────
        nullif(trim(notes), '')                                as notes,

        -- ── Tanggal ───────────────────────────────────────────────────────
        created_at                                             as created_at,

        -- ── Metadata / Audit ──────────────────────────────────────────────
        _ingested_at::timestamp without time zone              as loaded_at

    from source

    where
        gr_id             is not null
        and po_id         is not null
        and po_line_id    is not null
        and item_id       is not null
        and gr_date       is not null
        and quantity_received >= 0     -- kuantitas negatif tidak valid

)

select * from cleaned
