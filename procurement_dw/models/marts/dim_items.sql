{{
    config(
        materialized = 'table',
        description  = 'Dimensi item/barang. SCD Tipe 1 — perubahan harga atau nama menimpa record lama (overwrite).'
    )
}}

/*
=============================================================================
dim_items
=============================================================================
Layer   : Marts – Dimensi (table)
Grain   : 1 baris per item aktif maupun non-aktif di katalog

SCD Type: 1 (Overwrite)
  Perubahan atribut item (nama, harga, kategori) langsung menimpa record
  lama. Tidak ada histori perubahan yang disimpan.

  Kapan pakai SCD Type 1 vs Type 2?
  → Item: harga berubah sering, tapi analitik biasanya menggunakan harga
    aktual dari tabel fakta (bukan dari dimensi). Dimensi item cukup
    memberikan konteks label/kategori. SCD Type 1 sudah memadai.
  → Vendor: nama/lokasi vendor berubah jarang tapi berdampak besar pada
    pelaporan → butuh SCD Type 2 (lihat dim_vendors).

Surrogate Key:
  item_sk = MD5(item_id)
  Deterministik — stabil lintas run, FK fakta tidak perlu diperbarui.
=============================================================================
*/

with

source as (
    select * from {{ ref('stg_items') }}
),

final as (

    select
        -- ── Surrogate Key ─────────────────────────────────────────────────
        md5(item_id::text)                             as item_sk,

        -- ── Natural Key ───────────────────────────────────────────────────
        item_id,

        -- ── Identitas Item ────────────────────────────────────────────────
        item_code,
        item_name,
        item_category,
        unit_of_measure,

        -- ── Harga & Segmentasi ────────────────────────────────────────────
        -- Harga di dimensi = harga referensi saat ini (SCD Type 1)
        -- Harga aktual transaksi selalu diambil dari tabel fakta
        unit_price_idr                                 as current_unit_price_idr,
        price_tier,

        -- ── Pemesanan ─────────────────────────────────────────────────────
        min_order_qty,
        lead_time_days                                 as standard_lead_time_days,
        is_fast_moving,

        -- ── Status ────────────────────────────────────────────────────────
        is_active,

        -- ── Metadata SCD Type 1 ───────────────────────────────────────────
        created_at,
        loaded_at                                      as last_updated_at

    from source

)

select * from final
