{{
    config(
        materialized = 'view',
        description  = 'Staging: baris detail PO (tabel transaksi utama). Fix discount_pct, hitung net price, dan tambah flag diskon.'
    )
}}

/*
=============================================================================
stg_purchase_order_lines
=============================================================================
Sumber  : raw.purchase_order_lines
Layer   : Staging (view)
Tujuan  :
  - TRIM semua kolom TEXT
  - discount_pct (BIGINT di raw karena integer truncation) → NUMERIC(5,2)
  - unit_price / total_price (BIGINT) → NUMERIC untuk aritmatika
  - Cast _ingested_at (TEXT) → TIMESTAMP
  - Tambah kolom turunan:
      net_unit_price_idr  : harga satuan setelah diskon
      net_total_price_idr : nilai baris setelah diskon
      has_discount        : TRUE jika discount_pct > 0
      discount_amount_idr : nominal diskon dalam Rupiah
=============================================================================
*/

with

source as (

    select * from {{ source('raw_procurement', 'purchase_order_lines') }}

),

cleaned as (

    select
        -- ── Primary Key ───────────────────────────────────────────────────
        line_id                                                as line_id,

        -- ── Foreign Keys ──────────────────────────────────────────────────
        po_id                                                  as po_id,
        item_id                                                as item_id,

        -- ── Nomor Baris ───────────────────────────────────────────────────
        line_number                                            as line_number,

        -- ── Kuantitas & Satuan ────────────────────────────────────────────
        quantity_ordered                                       as quantity_ordered,
        trim(unit_of_measure)                                  as unit_of_measure,

        -- ── Harga ─────────────────────────────────────────────────────────
        -- Cast ke NUMERIC untuk operasi perkalian yang akurat
        unit_price::numeric                                    as unit_price_idr,

        -- discount_pct tersimpan sebagai BIGINT di raw (nilai integer 0–15)
        -- Cast ke NUMERIC(5,2) untuk representasi persentase yang benar
        coalesce(discount_pct, 0)::numeric(5,2)                as discount_pct,

        -- Harga satuan setelah diskon
        round(
            unit_price::numeric * (1 - coalesce(discount_pct, 0)::numeric / 100),
            0
        )                                                      as net_unit_price_idr,

        -- Total sebelum diskon (sesuai data raw)
        total_price::numeric                                   as gross_total_price_idr,

        -- Total setelah diskon (nilai ekonomis sesungguhnya dari baris PO)
        round(
            unit_price::numeric
            * quantity_ordered::numeric
            * (1 - coalesce(discount_pct, 0)::numeric / 100),
            0
        )                                                      as net_total_price_idr,

        -- Nominal diskon dalam Rupiah
        round(
            unit_price::numeric
            * quantity_ordered::numeric
            * coalesce(discount_pct, 0)::numeric / 100,
            0
        )                                                      as discount_amount_idr,

        -- ── Kolom Turunan ─────────────────────────────────────────────────
        case
            when coalesce(discount_pct, 0) > 0 then true
            else false
        end                                                    as has_discount,

        -- Segmentasi diskon
        case
            when coalesce(discount_pct, 0) = 0                then 'Tanpa Diskon'
            when coalesce(discount_pct, 0) between 1 and 5    then 'Diskon Kecil'
            when coalesce(discount_pct, 0) between 6 and 10   then 'Diskon Sedang'
            else                                                    'Diskon Besar'
        end                                                    as discount_tier,

        -- ── Pengiriman & Status ───────────────────────────────────────────
        delivery_date_expected                                 as delivery_date_expected,
        trim(line_status)                                      as line_status,

        -- ── Catatan ───────────────────────────────────────────────────────
        nullif(trim(notes), '')                                as notes,

        -- ── Tanggal ───────────────────────────────────────────────────────
        created_at                                             as created_at,

        -- ── Metadata / Audit ──────────────────────────────────────────────
        _ingested_at::timestamp without time zone              as loaded_at

    from source

    where
        line_id          is not null
        and po_id        is not null
        and item_id      is not null
        and quantity_ordered > 0        -- baris dengan kuantitas 0 dianggap invalid

)

select * from cleaned
