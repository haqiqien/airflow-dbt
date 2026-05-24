{{
    config(
        materialized = 'table',
        description  = 'Dimensi tanggal: kalender lengkap 2018–2027 untuk semua join berbasis tanggal di fakta.'
    )
}}

/*
=============================================================================
dim_date
=============================================================================
Layer   : Marts – Dimensi (table)
Grain   : 1 baris per hari kalender

Cakupan : 2018-01-01 s/d 2027-12-31
  - 2018–2022 : mencakup registered_date vendor tertua
  - 2023–2025 : rentang data transaksi aktual
  - 2026–2027 : buffer untuk analitik forward-looking

Surrogate Key:
  date_key = integer YYYYMMDD  (mis. 20240315 untuk 15 Maret 2024)
  Keunggulan vs sequence: human-readable, partisi-friendly, bisa
  digunakan langsung tanpa join di WHERE clause.

Kolom utama:
  - Hirarki kalender  : year → quarter → month → week → day
  - Label bahasa Indonesia: month_name_id, day_name_id
  - Flags analitik    : is_weekend, is_weekday, is_month_end, is_quarter_end
  - Tahun fiskal      : Indonesia menggunakan kalender Januari–Desember
=============================================================================
*/

with

date_spine as (
    -- generate_series menghasilkan satu baris per hari dalam rentang
    select
        generate_series(
            '2018-01-01'::date,
            '2027-12-31'::date,
            '1 day'::interval
        )::date  as full_date
),

final as (

    select
        -- ── Surrogate Key ────────────────────────────────────────────────
        -- Integer YYYYMMDD: cepat, mudah dibaca, tidak perlu join untuk filter
        to_char(full_date, 'YYYYMMDD')::integer        as date_key,

        -- ── Tanggal Lengkap ───────────────────────────────────────────────
        full_date,
        to_char(full_date, 'DD Month YYYY')            as date_label,       -- "15 Maret 2024"

        -- ── TAHUN ─────────────────────────────────────────────────────────
        extract(year from full_date)::integer          as year,
        extract(isoyear from full_date)::integer       as iso_year,         -- untuk kalkulasi minggu ISO

        -- ── KUARTAL ───────────────────────────────────────────────────────
        extract(quarter from full_date)::integer       as quarter_number,
        'Q' || extract(quarter from full_date)::text   as quarter_label,    -- "Q1", "Q2", ...
        extract(year from full_date)::text
          || '-Q' || extract(quarter from full_date)::text
                                                       as year_quarter,     -- "2024-Q1"

        -- ── BULAN ─────────────────────────────────────────────────────────
        extract(month from full_date)::integer         as month_number,
        to_char(full_date, 'Mon')                      as month_short_en,   -- "Jan", "Feb", ...

        -- Label bulan dalam Bahasa Indonesia
        case extract(month from full_date)::integer
            when  1 then 'Januari'    when  2 then 'Februari'
            when  3 then 'Maret'      when  4 then 'April'
            when  5 then 'Mei'        when  6 then 'Juni'
            when  7 then 'Juli'       when  8 then 'Agustus'
            when  9 then 'September'  when 10 then 'Oktober'
            when 11 then 'November'   when 12 then 'Desember'
        end                                            as month_name_id,

        -- Format YYYY-MM untuk GROUP BY bulanan
        to_char(full_date, 'YYYY-MM')                  as year_month,       -- "2024-03"

        -- ── MINGGU ────────────────────────────────────────────────────────
        extract(week from full_date)::integer          as week_of_year,     -- ISO week (1–53)
        extract(isoyear from full_date)::text
          || '-W' || to_char(extract(week from full_date)::integer, 'FM00')
                                                       as year_week,        -- "2024-W12"

        -- ── HARI ──────────────────────────────────────────────────────────
        extract(day  from full_date)::integer          as day_of_month,
        extract(doy  from full_date)::integer          as day_of_year,

        -- PostgreSQL: extract(dow) → 0=Minggu, 1=Senin, ..., 6=Sabtu
        extract(dow  from full_date)::integer          as day_of_week_sun0,

        -- ISO: 1=Senin, ..., 7=Minggu (lebih umum di analitik)
        extract(isodow from full_date)::integer        as day_of_week_iso,

        -- Label hari dalam Bahasa Indonesia
        case extract(isodow from full_date)::integer
            when 1 then 'Senin'    when 2 then 'Selasa'
            when 3 then 'Rabu'     when 4 then 'Kamis'
            when 5 then 'Jumat'    when 6 then 'Sabtu'
            when 7 then 'Minggu'
        end                                            as day_name_id,

        case extract(isodow from full_date)::integer
            when 1 then 'Sen' when 2 then 'Sel' when 3 then 'Rab'
            when 4 then 'Kam' when 5 then 'Jum' when 6 then 'Sab'
            when 7 then 'Min'
        end                                            as day_short_id,

        -- ── FLAGS BOOLEAN ─────────────────────────────────────────────────
        -- Akhir pekan (Sabtu=6 / Minggu=0 dalam Sun-0 convention)
        extract(dow from full_date)::integer in (0, 6) as is_weekend,
        extract(dow from full_date)::integer not in (0, 6) as is_weekday,

        -- Hari pertama & terakhir bulan
        full_date = date_trunc('month', full_date)::date
                                                       as is_first_day_of_month,
        full_date = (date_trunc('month', full_date)
                     + interval '1 month'
                     - interval '1 day')::date         as is_last_day_of_month,

        -- Hari terakhir kuartal
        full_date = (date_trunc('quarter', full_date)
                     + interval '3 months'
                     - interval '1 day')::date         as is_last_day_of_quarter,

        -- Hari pertama & terakhir tahun
        full_date = date_trunc('year', full_date)::date
                                                       as is_first_day_of_year,
        full_date = (date_trunc('year', full_date)
                     + interval '1 year'
                     - interval '1 day')::date         as is_last_day_of_year,

        -- ── TAHUN FISKAL ──────────────────────────────────────────────────
        -- Indonesia: tahun fiskal = tahun kalender (Januari – Desember)
        extract(year    from full_date)::integer       as fiscal_year,
        extract(quarter from full_date)::integer       as fiscal_quarter,
        extract(month   from full_date)::integer       as fiscal_month

    from date_spine

)

select * from final
