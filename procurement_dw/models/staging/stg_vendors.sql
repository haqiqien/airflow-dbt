{{
    config(
        materialized = 'view',
        description  = 'Staging: master data vendor. Fix postal_code (BIGINT→TEXT), normalisasi email, dan flag vendor aktif.'
    )
}}

/*
=============================================================================
stg_vendors
=============================================================================
Sumber  : raw.vendors
Layer   : Staging (view)
Tujuan  :
  - TRIM semua kolom TEXT
  - LOWER + TRIM pada kolom email
  - postal_code: BIGINT → TEXT dengan zero-padding (LPAD 5 digit)
    Contoh: 6132 → '06132'
  - rating: DOUBLE PRECISION → NUMERIC(3,1) untuk konsistensi
  - Cast _ingested_at (TEXT) → TIMESTAMP
  - Tambah kolom turunan: is_goods_vendor, is_service_vendor
  - Tidak memfilter vendor non-aktif (dilakukan di layer marts)
=============================================================================
*/

with

source as (

    select * from {{ source('raw_procurement', 'vendors') }}

),

cleaned as (

    select
        -- ── Primary Key ───────────────────────────────────────────────────
        vendor_id                                              as vendor_id,

        -- ── Kode & Identitas ──────────────────────────────────────────────
        trim(vendor_code)                                      as vendor_code,
        trim(vendor_name)                                      as vendor_name,
        trim(vendor_category)                                  as vendor_category,
        trim(vendor_type)                                      as vendor_type,

        -- ── Alamat ────────────────────────────────────────────────────────
        trim(address)                                          as address,
        trim(city)                                             as city,
        trim(province)                                         as province,

        -- postal_code: raw menyimpan sebagai BIGINT karena pandas dtype inference
        -- Cast ke TEXT dan pad ke 5 digit untuk mempertahankan format kode pos Indonesia
        lpad(postal_code::text, 5, '0')                        as postal_code,

        -- ── Kontak ────────────────────────────────────────────────────────
        trim(phone)                                            as phone,

        -- Email: lowercase + trim untuk konsistensi pencocokan
        lower(trim(email))                                     as email,
        trim(npwp)                                             as npwp,

        -- ── Syarat Bisnis ─────────────────────────────────────────────────
        trim(payment_terms)                                    as payment_terms,

        -- rating: bulatkan ke 1 desimal
        round(rating::numeric, 1)                              as vendor_rating,

        -- ── Status ────────────────────────────────────────────────────────
        is_active                                              as is_active,

        -- ── Kolom Turunan ─────────────────────────────────────────────────
        -- Memudahkan filter di layer marts tanpa perlu string comparison
        case
            when trim(vendor_type) in ('Barang', 'Barang & Jasa')
            then true else false
        end                                                    as is_goods_vendor,

        case
            when trim(vendor_type) in ('Jasa', 'Barang & Jasa')
            then true else false
        end                                                    as is_service_vendor,

        -- ── Tanggal ───────────────────────────────────────────────────────
        registered_date                                        as registered_date,
        created_at                                             as created_at,

        -- ── Metadata / Audit ──────────────────────────────────────────────
        _ingested_at::timestamp without time zone              as loaded_at

    from source

    where
        vendor_id   is not null
        and trim(vendor_name) <> ''

)

select * from cleaned
