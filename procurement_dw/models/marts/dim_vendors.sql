{{
    config(
        materialized = 'table',
        description  = 'Dimensi vendor dengan SCD Tipe 2: melacak histori perubahan nama perusahaan, kota, provinsi, dan syarat pembayaran.'
    )
}}

/*
=============================================================================
dim_vendors  —  SCD Type 2
=============================================================================
Layer   : Marts – Dimensi (table)
Grain   : 1 baris per versi vendor (vendor_id + dbt_valid_from)

SCD Type: 2 (Add Row)
  Kolom yang dilacak historisnya (tracked attributes):
    • vendor_name    — rebranding perusahaan
    • city           — perpindahan kantor
    • province       — perpindahan kantor lintas provinsi
    • payment_terms  — renegosiasi syarat pembayaran

  Saat salah satu kolom di atas berubah:
    → Record lama diupdate: dbt_valid_to = hari ini - 1 hari
    → Record baru diinsert: dbt_valid_from = hari ini, dbt_valid_to = NULL

Surrogate Key:
  vendor_sk = MD5(vendor_id || '|' || dbt_valid_from)
  Unik per versi; tabel fakta join menggunakan vendor_sk pada record
  yang is_current_record = TRUE (atau range-join berdasarkan tanggal).

─────────────────────────────────────────────────────────────────────────────
IMPLEMENTASI PRODUKSI vs SIMULASI INI
─────────────────────────────────────────────────────────────────────────────
Dalam pipeline produksi, SCD Type 2 dikelola oleh dbt snapshots.
Contoh konfigurasi di snapshots/vendors_snapshot.sql:

    -- unique_key    : 'vendor_id'
    -- strategy      : 'check'
    -- check_cols    : ['vendor_name', 'city', 'province', 'payment_terms']
    -- target_schema : 'snapshots'

Dimensi ini kemudian cukup mereferensikan snapshot tersebut.

Karena data kita bersifat sintetis tanpa CDC, model ini mensimulasikan
histori dengan dua pendekatan:
  1. Vendor yang terdaftar sebelum 2021 → punya versi "sebelum rebranding"
     (versi lama dibuat dengan tanda '(Versi Lama)' pada nama & kota asal).
  2. Semua vendor → versi current (is_current_record = TRUE).

Proporsi vendor dengan histori: ~vendor_id % 3 = 0 (~33% dari vendor lama).
=============================================================================
*/

with

current_vendors as (
    select * from {{ ref('stg_vendors') }}
),

-- ── Versi Historis (Simulasi) ─────────────────────────────────────────────
-- Mewakili kondisi vendor SEBELUM perubahan nama/lokasi/syarat pembayaran.
-- Dalam produksi: baris ini berasal dari dbt snapshot / audit log.
historical_versions as (

    select
        vendor_id,
        vendor_code,

        -- Simulasi nama lama sebelum rebranding
        regexp_replace(vendor_name, ' (Tbk\.?|PT )', ' ') || ' (Versi Lama)'
                                                       as vendor_name,

        vendor_category,
        vendor_type,
        address,

        -- Simulasi: kantor lama di kota asal (Jakarta sebagai default historis)
        'Jakarta Pusat'                                as city,
        'DKI Jakarta'                                  as province,
        postal_code,
        phone,
        email,
        npwp,

        -- Simulasi: syarat pembayaran lama lebih ketat (Net 60)
        'Net 60'                                       as payment_terms,
        vendor_rating,
        is_active,
        is_goods_vendor,
        is_service_vendor,
        registered_date,
        created_at,

        -- Versi historis valid: dari tanggal registrasi s/d 2021-12-31
        registered_date                                as dbt_valid_from,
        '2021-12-31'::date                             as dbt_valid_to,
        false                                          as is_current_record,
        1                                              as record_version

    from current_vendors
    where
        -- Hanya vendor lama (terdaftar sebelum 2021) yang disimulasikan punya histori
        registered_date < '2021-01-01'
        -- ~33% dari vendor lama untuk simulasi realistis
        and vendor_id % 3 = 0
),

-- ── Versi Current (semua vendor) ─────────────────────────────────────────
current_versions as (

    select
        vendor_id,
        vendor_code,
        vendor_name,
        vendor_category,
        vendor_type,
        address,
        city,
        province,
        postal_code,
        phone,
        email,
        npwp,
        payment_terms,
        vendor_rating,
        is_active,
        is_goods_vendor,
        is_service_vendor,
        registered_date,
        created_at,

        -- Current record valid mulai:
        --   • Vendor yang punya histori (% 3 = 0) → valid dari 2022-01-01
        --   • Vendor baru (tdk punya histori)     → valid dari registered_date
        case
            when registered_date < '2021-01-01' and vendor_id % 3 = 0
            then '2022-01-01'::date
            else registered_date
        end                                            as dbt_valid_from,

        null::date                                     as dbt_valid_to,  -- open-ended
        true                                           as is_current_record,

        -- Version number: 2 jika punya histori, 1 jika baru
        case
            when registered_date < '2021-01-01' and vendor_id % 3 = 0
            then 2
            else 1
        end                                            as record_version

    from current_vendors

),

-- ── Gabungkan semua versi ─────────────────────────────────────────────────
all_versions as (
    select * from historical_versions
    union all
    select * from current_versions
),

-- ── Tambah Surrogate Key ─────────────────────────────────────────────────
final as (

    select
        -- ── Surrogate Key: unik per versi record ──────────────────────────
        -- Format: MD5(vendor_id | dbt_valid_from)
        md5(
            vendor_id::text || '|' || dbt_valid_from::text
        )                                              as vendor_sk,

        -- ── Kolom SCD Type 2 ──────────────────────────────────────────────
        dbt_valid_from,
        dbt_valid_to,
        is_current_record,
        record_version,

        -- ── Natural Key ───────────────────────────────────────────────────
        vendor_id,

        -- ── Atribut Tracked (bisa berubah antar versi) ────────────────────
        vendor_name,
        city,
        province,
        payment_terms,

        -- ── Atribut Non-Tracked (tidak memicu versi baru) ─────────────────
        vendor_code,
        vendor_category,
        vendor_type,
        address,
        postal_code,
        phone,
        email,
        npwp,
        vendor_rating,
        is_active,
        is_goods_vendor,
        is_service_vendor,

        -- ── Metadata ──────────────────────────────────────────────────────
        registered_date,
        created_at

    from all_versions

)

select * from final
