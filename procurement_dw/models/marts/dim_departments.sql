{{
    config(
        materialized = 'table',
        description  = 'Dimensi departemen. SCD Tipe 0 (static) — perubahan departemen sangat jarang dan tidak dilacak historisnya.'
    )
}}

/*
=============================================================================
dim_departments
=============================================================================
Layer   : Marts – Dimensi (table)
Grain   : 1 baris per departemen

SCD Type: 0 (Fixed / Static)
  Departemen dianggap sangat stabil. Perubahan seperti restrukturisasi
  ditangani dengan proses operasional terpisah (bukan CDC otomatis).
  Jika perlu histori, upgrade ke SCD Type 1 cukup (full overwrite).

Surrogate Key:
  department_sk = MD5(department_id)
  Deterministik — tidak berubah antar run sehingga FK di fakta tetap stabil.
=============================================================================
*/

with

source as (
    select * from {{ ref('stg_departments') }}
),

final as (

    select
        -- ── Surrogate Key ─────────────────────────────────────────────────
        md5(department_id::text)                       as department_sk,

        -- ── Natural Key ───────────────────────────────────────────────────
        department_id,

        -- ── Atribut Identitas ─────────────────────────────────────────────
        department_code,
        department_name,
        cost_center,

        -- ── Lokasi & Manajemen ────────────────────────────────────────────
        location,
        manager_name,

        -- ── Finansial ─────────────────────────────────────────────────────
        budget_annual_idr,

        -- Segmentasi departemen berdasarkan besaran anggaran
        -- Berguna untuk filter/slice di dashboard tanpa join tambahan
        case
            when budget_annual_idr >= 5000000000 then 'Anggaran Besar'   -- ≥ 5 Miliar
            when budget_annual_idr >= 1000000000 then 'Anggaran Menengah' -- 1–5 Miliar
            else                                      'Anggaran Kecil'    -- < 1 Miliar
        end                                            as budget_tier,

        -- ── Metadata ──────────────────────────────────────────────────────
        created_at,
        loaded_at

    from source

)

select * from final
