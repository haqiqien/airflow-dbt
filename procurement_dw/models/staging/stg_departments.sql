{{
    config(
        materialized = 'view',
        description  = 'Staging: master data departemen. Membersihkan whitespace, cast tipe, dan menambahkan kolom audit.'
    )
}}

/*
=============================================================================
stg_departments
=============================================================================
Sumber  : raw.departments
Layer   : Staging (view)
Tujuan  :
  - Standardisasi nama kolom (sudah snake_case, tidak ada rename besar)
  - TRIM whitespace pada seluruh kolom TEXT
  - Cast _ingested_at (TEXT) → TIMESTAMP
  - Cast budget_annual (BIGINT) → NUMERIC untuk presisi analitik
  - Tambah kolom audit: loaded_at
=============================================================================
*/

with

source as (

    select * from {{ source('raw_procurement', 'departments') }}

),

cleaned as (

    select
        -- ── Primary Key ───────────────────────────────────────────────────
        department_id                                          as department_id,

        -- ── Kode & Nama ───────────────────────────────────────────────────
        trim(department_code)                                  as department_code,
        trim(department_name)                                  as department_name,
        trim(cost_center)                                      as cost_center,

        -- ── Informasi Kontak & Lokasi ─────────────────────────────────────
        trim(location)                                         as location,
        trim(manager_name)                                     as manager_name,

        -- ── Finansial ─────────────────────────────────────────────────────
        -- Cast ke NUMERIC agar bisa digunakan dalam agregasi presisi tinggi
        budget_annual::numeric                                 as budget_annual_idr,

        -- ── Tanggal ───────────────────────────────────────────────────────
        -- created_at sudah DATE di raw; tidak perlu cast
        created_at                                             as created_at,

        -- ── Metadata / Audit ──────────────────────────────────────────────
        -- _ingested_at disimpan sebagai TEXT di raw; cast ke TIMESTAMP
        _ingested_at::timestamp without time zone              as loaded_at

    from source

    where
        -- Pastikan baris tidak orphan: department_id dan nama wajib ada
        department_id  is not null
        and trim(department_name) <> ''

)

select * from cleaned
