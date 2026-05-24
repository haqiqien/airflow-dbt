{{
    config(
        materialized = 'view',
        description  = 'Staging: master katalog item. Cast harga ke NUMERIC, tambah flag kategori harga.'
    )
}}

/*
=============================================================================
stg_items
=============================================================================
Sumber  : raw.items
Layer   : Staging (view)
Tujuan  :
  - TRIM semua kolom TEXT
  - unit_price (BIGINT) → NUMERIC untuk operasi aritmatika presisi
  - Cast _ingested_at (TEXT) → TIMESTAMP
  - Tambah kolom turunan:
      price_tier    : klasifikasi harga (Murah / Menengah / Mahal)
      is_fast_moving: TRUE jika lead_time_days ≤ 3
  - Tidak memfilter item non-aktif (dilakukan di layer marts)
=============================================================================
*/

with

source as (

    select * from {{ source('raw_procurement', 'items') }}

),

cleaned as (

    select
        -- ── Primary Key ───────────────────────────────────────────────────
        item_id                                                as item_id,

        -- ── Kode & Deskripsi ──────────────────────────────────────────────
        trim(item_code)                                        as item_code,
        trim(item_name)                                        as item_name,
        trim(item_category)                                    as item_category,

        -- ── Satuan ────────────────────────────────────────────────────────
        trim(unit_of_measure)                                  as unit_of_measure,

        -- ── Harga ─────────────────────────────────────────────────────────
        -- Cast ke NUMERIC agar bisa dikalikan qty (BIGINT × NUMERIC → NUMERIC)
        unit_price::numeric                                    as unit_price_idr,

        -- ── Pemesanan ─────────────────────────────────────────────────────
        min_order_qty                                          as min_order_qty,
        lead_time_days                                         as lead_time_days,

        -- ── Status ────────────────────────────────────────────────────────
        is_active                                              as is_active,

        -- ── Kolom Turunan ─────────────────────────────────────────────────
        -- Segmentasi harga berdasarkan unit_price
        case
            when unit_price < 100000              then 'Murah'
            when unit_price between 100000
                               and  1000000       then 'Menengah'
            else                                       'Mahal'
        end                                                    as price_tier,

        -- Item yang bisa dikirim cepat (≤3 hari) berguna untuk analisis urgency
        case
            when lead_time_days <= 3 then true
            else false
        end                                                    as is_fast_moving,

        -- ── Tanggal ───────────────────────────────────────────────────────
        created_at                                             as created_at,

        -- ── Metadata / Audit ──────────────────────────────────────────────
        _ingested_at::timestamp without time zone              as loaded_at

    from source

    where
        item_id   is not null
        and trim(item_name) <> ''

)

select * from cleaned
