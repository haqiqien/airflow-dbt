{{
    config(
        materialized = 'view',
        description  = 'Staging: header Purchase Order (PO). Hitung expected lead time, flag status, dan komponen tanggal.'
    )
}}

/*
=============================================================================
stg_purchase_orders
=============================================================================
Sumber  : raw.purchase_orders
Layer   : Staging (view)
Tujuan  :
  - TRIM semua kolom TEXT
  - Cast total_amount (BIGINT) → NUMERIC
  - Cast _ingested_at (TEXT) → TIMESTAMP
  - Tambah kolom turunan:
      po_year / po_month / po_year_month: partisi analitik
      expected_lead_time_days : jarak hari PO → expected_delivery_date
      approval_lag_days       : jarak hari po_date → approval_date
      is_cancelled            : TRUE jika status = 'Dibatalkan'
      is_completed            : TRUE jika status = 'Selesai'
      is_open                 : TRUE jika status = 'Terbuka'
      is_partial              : TRUE jika status = 'Sebagian Diterima'
=============================================================================
*/

with

source as (

    select * from {{ source('raw_procurement', 'purchase_orders') }}

),

cleaned as (

    select
        -- ── Primary Key ───────────────────────────────────────────────────
        po_id                                                  as po_id,

        -- ── Nomor & Tanggal ───────────────────────────────────────────────
        trim(po_number)                                        as po_number,
        po_date                                                as po_date,

        -- Komponen tanggal untuk analisis tren pengadaan
        extract(year  from po_date)::int                       as po_year,
        extract(month from po_date)::int                       as po_month,
        to_char(po_date, 'YYYY-MM')                            as po_year_month,

        -- ── Foreign Keys ──────────────────────────────────────────────────
        pr_id                                                  as pr_id,
        vendor_id                                              as vendor_id,
        department_id                                          as department_id,

        -- ── Pengiriman ────────────────────────────────────────────────────
        expected_delivery_date                                 as expected_delivery_date,

        -- Expected lead time: berapa hari dari terbit PO sampai tanggal yang dijanjikan
        (expected_delivery_date - po_date)::int                as expected_lead_time_days,

        -- ── Pembayaran & Status ───────────────────────────────────────────
        trim(payment_terms)                                    as payment_terms,
        trim(status)                                           as po_status,

        -- ── Alamat & Approval ─────────────────────────────────────────────
        trim(shipping_address)                                 as shipping_address,
        trim(approved_by)                                      as approved_by,
        approval_date                                          as approval_date,

        -- Berapa hari dari po_date sampai approval (normalnya 0–3 hari)
        case
            when approval_date is not null
            then (approval_date - po_date)::int
            else null
        end                                                    as approval_lag_days,

        -- ── Kolom Turunan: Status Flags ───────────────────────────────────
        case when trim(status) = 'Terbuka'            then true else false end  as is_open,
        case when trim(status) = 'Sebagian Diterima'  then true else false end  as is_partial,
        case when trim(status) = 'Selesai'            then true else false end  as is_completed,
        case when trim(status) = 'Dibatalkan'         then true else false end  as is_cancelled,

        -- PO aktif = belum selesai dan belum dibatalkan
        case
            when trim(status) not in ('Selesai', 'Dibatalkan') then true
            else false
        end                                                    as is_active_po,

        -- ── Finansial ─────────────────────────────────────────────────────
        coalesce(total_amount, 0)::numeric                     as total_amount_idr,

        -- ── Catatan ───────────────────────────────────────────────────────
        nullif(trim(notes), '')                                as notes,

        -- ── Tanggal ───────────────────────────────────────────────────────
        created_at                                             as created_at,

        -- ── Metadata / Audit ──────────────────────────────────────────────
        _ingested_at::timestamp without time zone              as loaded_at

    from source

    where
        po_id   is not null
        and po_date is not null

)

select * from cleaned
