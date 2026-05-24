{{
    config(
        materialized = 'view',
        description  = 'Staging: header Purchase Request (PR). Standardisasi status, tambah flag approval, dan hitung approval lead time.'
    )
}}

/*
=============================================================================
stg_purchase_requests
=============================================================================
Sumber  : raw.purchase_requests
Layer   : Staging (view)
Tujuan  :
  - TRIM semua kolom TEXT
  - LOWER + TRIM pada requester_email
  - Cast total_amount (BIGINT) → NUMERIC
  - Cast _ingested_at (TEXT) → TIMESTAMP
  - Tambah kolom turunan:
      is_approved       : TRUE jika status = 'Disetujui'
      is_rejected       : TRUE jika status = 'Ditolak'
      approval_lead_days: selisih hari antara pr_date dan approval_date
      pr_year / pr_month: komponen tanggal untuk partisi analitik
=============================================================================
*/

with

source as (

    select * from {{ source('raw_procurement', 'purchase_requests') }}

),

cleaned as (

    select
        -- ── Primary Key ───────────────────────────────────────────────────
        pr_id                                                  as pr_id,

        -- ── Nomor & Tanggal ───────────────────────────────────────────────
        trim(pr_number)                                        as pr_number,
        pr_date                                                as pr_date,

        -- Komponen tanggal — berguna untuk GROUP BY tanpa fungsi di marts
        extract(year  from pr_date)::int                       as pr_year,
        extract(month from pr_date)::int                       as pr_month,
        to_char(pr_date, 'YYYY-MM')                            as pr_year_month,

        -- ── Foreign Keys ──────────────────────────────────────────────────
        department_id                                          as department_id,

        -- ── Pemohon ───────────────────────────────────────────────────────
        trim(requester_name)                                   as requester_name,
        lower(trim(requester_email))                           as requester_email,
        trim(purpose)                                          as purpose,

        -- ── Prioritas & Status ────────────────────────────────────────────
        trim(priority)                                         as priority,
        trim(status)                                           as pr_status,

        -- ── Approval ──────────────────────────────────────────────────────
        trim(approved_by)                                      as approved_by,
        approval_date                                          as approval_date,

        -- ── Kolom Turunan ─────────────────────────────────────────────────
        -- Boolean flags untuk memudahkan filter di marts
        case when trim(status) = 'Disetujui'        then true  else false end  as is_approved,
        case when trim(status) = 'Ditolak'          then true  else false end  as is_rejected,
        case when trim(status) = 'Pending Approval' then true  else false end  as is_pending,

        -- Berapa hari dari pengajuan PR sampai mendapat keputusan approval
        -- NULL jika belum ada approval_date
        case
            when approval_date is not null
            then (approval_date - pr_date)::int
            else null
        end                                                    as approval_lead_days,

        -- ── Finansial ─────────────────────────────────────────────────────
        -- total_amount bisa NULL jika PR belum memiliki PO
        coalesce(total_amount, 0)::numeric                     as total_amount_idr,

        -- ── Catatan ───────────────────────────────────────────────────────
        -- NULLIF untuk mengubah string kosong menjadi NULL yang eksplisit
        nullif(trim(notes), '')                                as notes,

        -- ── Tanggal ───────────────────────────────────────────────────────
        created_at                                             as created_at,

        -- ── Metadata / Audit ──────────────────────────────────────────────
        _ingested_at::timestamp without time zone              as loaded_at

    from source

    where
        pr_id is not null
        and pr_date is not null

)

select * from cleaned
