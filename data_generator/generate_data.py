"""
=============================================================================
Procurement Synthetic Data Generator
=============================================================================
Menghasilkan data sintetis domain Procurement dengan locale id_ID.

Tabel yang dihasilkan:
  1. departments          - Master departemen perusahaan
  2. vendors              - Master vendor/pemasok
  3. items                - Master barang/jasa
  4. purchase_requests    - Header permintaan pembelian (PR)
  5. purchase_orders      - Header pesanan pembelian (PO)
  6. purchase_order_lines - Baris detail PO (tabel transaksi utama, >=10.000 baris)
  7. goods_receipts       - Penerimaan barang (GR) dengan lead time & variasi qty

Relasi FK:
  purchase_requests.department_id  → departments.department_id
  purchase_orders.pr_id            → purchase_requests.pr_id
  purchase_orders.vendor_id        → vendors.vendor_id
  purchase_orders.department_id    → departments.department_id
  purchase_order_lines.po_id       → purchase_orders.po_id
  purchase_order_lines.item_id     → items.item_id
  goods_receipts.po_id             → purchase_orders.po_id
  goods_receipts.po_line_id        → purchase_order_lines.line_id
  goods_receipts.item_id           → items.item_id

Output: ./data/*.csv
=============================================================================
"""

import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# INISIALISASI
# ---------------------------------------------------------------------------
fake = Faker("id_ID")
Faker.seed(42)
random.seed(42)
np.random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# KONFIGURASI VOLUME DATA
# ---------------------------------------------------------------------------
N_DEPARTMENTS = 30
N_VENDORS     = 150
N_ITEMS       = 300
N_PR          = 2_500   # Purchase Requests
N_PO          = 2_800   # Purchase Orders  (sebagian besar dari PR disetujui)
# PO Lines: N_PO × rata-rata 5 baris ≈ 14.000 baris  ✓ memenuhi syarat ≥10.000
# Goods Receipts: ~70% PO memiliki GR × rata-rata 5 baris ≈ ~9.800 baris

DATE_START = datetime(2023, 1, 1)
DATE_END   = datetime(2025, 12, 31)

# ---------------------------------------------------------------------------
# REFERENSI DATA INDONESIA
# ---------------------------------------------------------------------------
INDONESIA_PROVINCES = [
    "Aceh", "Sumatera Utara", "Sumatera Barat", "Riau", "Kepulauan Riau",
    "Jambi", "Sumatera Selatan", "Bangka Belitung", "Bengkulu", "Lampung",
    "DKI Jakarta", "Jawa Barat", "Banten", "Jawa Tengah", "DI Yogyakarta",
    "Jawa Timur", "Kalimantan Barat", "Kalimantan Tengah", "Kalimantan Selatan",
    "Kalimantan Timur", "Kalimantan Utara", "Sulawesi Utara", "Gorontalo",
    "Sulawesi Tengah", "Sulawesi Barat", "Sulawesi Selatan", "Sulawesi Tenggara",
    "Maluku", "Maluku Utara", "Bali", "Nusa Tenggara Barat", "Nusa Tenggara Timur",
    "Papua", "Papua Barat",
]

DEPARTMENT_NAMES = [
    "Pengadaan & Pembelian", "Operasional", "Keuangan & Akuntansi",
    "Sumber Daya Manusia", "Teknologi Informasi", "Pemasaran & Penjualan",
    "Produksi & Manufaktur", "Logistik & Distribusi", "Riset & Pengembangan",
    "Hukum & Kepatuhan", "Manajemen Mutu", "Layanan Pelanggan",
    "Fasilitas & Umum", "Kesehatan & Keselamatan Kerja", "Audit Internal",
    "Perencanaan Strategis", "Hubungan Masyarakat", "Ekspor & Impor",
    "Manajemen Proyek", "Inovasi & Transformasi Digital", "Warehouse & Pergudangan",
    "Supply Chain Management", "Maintenance & Teknik", "Pelatihan & Pengembangan SDM",
    "Administrasi Umum", "Rekayasa & Proses Bisnis", "Business Intelligence",
    "Keberlanjutan & CSR", "Keamanan Perusahaan", "Manajemen Aset",
]

# Katalog item per kategori: (nama_item, uom, harga_min, harga_maks)
ITEM_CATALOG = {
    "Alat Tulis Kantor": [
        ("Kertas HVS A4 80gr", "Rim",   45_000,   80_000),
        ("Pulpen Ballpoint",   "Lusin",  18_000,   50_000),
        ("Spidol Whiteboard",  "Box",    35_000,   90_000),
        ("Tinta Printer",      "Botol",  50_000,  200_000),
        ("Staples No.10",      "Box",    15_000,   35_000),
        ("Map Snelhechter",    "Lusin",  30_000,   80_000),
        ("Buku Tulis 100 lbr", "Lusin",  40_000,   90_000),
        ("Amplop Coklat",      "Box",    25_000,   60_000),
        ("Penggaris 30cm",     "Pcs",     8_000,   20_000),
        ("Kalkulator Saku",    "Pcs",    50_000,  200_000),
        ("Klip Kertas",        "Box",    10_000,   25_000),
        ("Gunting",            "Pcs",    15_000,   45_000),
    ],
    "IT & Teknologi": [
        ("Laptop Business 14\"",    "Unit",  8_000_000, 25_000_000),
        ("Monitor LED 24\"",        "Unit",  2_500_000,  6_000_000),
        ("Keyboard Wireless",       "Unit",    250_000,    900_000),
        ("Mouse Optical",           "Unit",    100_000,    500_000),
        ("Headset USB",             "Unit",    200_000,    800_000),
        ("Flash Drive 64GB",        "Unit",     80_000,    250_000),
        ("Hard Disk Eksternal 2TB", "Unit",    650_000,  1_500_000),
        ("Webcam HD 1080p",         "Unit",    350_000,    900_000),
        ("Switch Jaringan 24-Port", "Unit",  1_500_000,  5_000_000),
        ("UPS 1200VA",              "Unit",    900_000,  2_500_000),
        ("Kabel LAN Cat6 (50m)",    "Roll",    200_000,    500_000),
        ("SSD 512GB",               "Unit",    700_000,  1_800_000),
    ],
    "Furnitur Kantor": [
        ("Kursi Ergonomis Mesh",   "Unit",    800_000,  4_500_000),
        ("Meja Kerja L-Shape",     "Unit",  1_200_000,  5_000_000),
        ("Lemari Arsip Besi",      "Unit",    900_000,  3_500_000),
        ("Rak Buku 5 Susun",       "Unit",    400_000,  1_800_000),
        ("Papan Tulis Whiteboard", "Unit",    350_000,  2_500_000),
        ("Proyektor 3000 Lumens",  "Unit",  3_500_000, 12_000_000),
        ("Loker Karyawan 4 Pintu", "Unit",    700_000,  2_800_000),
        ("Sofa Tunggu 3-Seater",   "Unit",  1_500_000,  7_000_000),
        ("Meja Rapat Oval 12-Org", "Unit",  4_000_000, 15_000_000),
        ("Filling Cabinet 4-Laci", "Unit",  1_100_000,  4_000_000),
    ],
    "Peralatan Listrik": [
        ("Kabel Roll 5m",          "Pcs",    80_000,   250_000),
        ("Stop Kontak 5-Lubang",   "Pcs",    50_000,   200_000),
        ("Lampu LED 18W",          "Pcs",    35_000,   120_000),
        ("Baterai AA (4-pack)",    "Pack",   20_000,    60_000),
        ("MCB Circuit Breaker",    "Pcs",    80_000,   350_000),
        ("Stabilizer Listrik 5KVA","Unit", 1_200_000, 4_500_000),
        ("Genset Portable 2200W",  "Unit", 4_000_000,12_000_000),
        ("Lampu Emergency",        "Pcs",   150_000,   500_000),
        ("Timer Otomatis",         "Pcs",    80_000,   250_000),
    ],
    "Bahan Kebersihan": [
        ("Sabun Cuci Tangan 5L",   "Galon",  90_000,  200_000),
        ("Pembersih Lantai 5L",    "Galon",  70_000,  160_000),
        ("Tisu Gulung (12-roll)",  "Pack",   45_000,  100_000),
        ("Kantong Sampah 60L",     "Roll",   30_000,   80_000),
        ("Sapu Gagang Panjang",    "Pcs",    40_000,  120_000),
        ("Alat Pel Lantai",        "Set",    80_000,  300_000),
        ("Disinfektan 5L",         "Galon", 120_000,  300_000),
        ("Sarung Tangan Karet",    "Lusin",  80_000,  200_000),
        ("Sikat Toilet",           "Pcs",    25_000,   80_000),
        ("Pengharum Ruangan",      "Pcs",    30_000,   90_000),
    ],
    "Peralatan Keselamatan": [
        ("Masker N95",             "Box",   150_000,  500_000),
        ("Hand Sanitizer 500ml",   "Botol",  40_000,  120_000),
        ("Kotak P3K Lengkap",      "Set",   250_000,  800_000),
        ("Termometer Digital",     "Pcs",   100_000,  350_000),
        ("Helm Keselamatan",       "Pcs",   120_000,  500_000),
        ("Rompi Safety",           "Pcs",    80_000,  300_000),
        ("Sepatu Safety",          "Pasang",400_000,1_500_000),
        ("APAR 3kg",               "Unit",  350_000,  900_000),
        ("Tali Safety",            "Meter",  50_000,  200_000),
        ("Kacamata Safety",        "Pcs",    50_000,  200_000),
    ],
    "Spare Part & Mesin": [
        ("Filter Oli Mesin",       "Pcs",    80_000,  250_000),
        ("Oli Mesin 20L",          "Galon", 500_000,1_500_000),
        ("Baut & Mur Set",         "Set",   100_000,  400_000),
        ("Bearing SKF",            "Pcs",   200_000,  800_000),
        ("V-Belt",                 "Pcs",   120_000,  450_000),
        ("Seal Kit Hidrolik",      "Set",   350_000,1_500_000),
        ("Pompa Air 0.5HP",        "Unit",  500_000,2_000_000),
        ("Kompresor Angin 1HP",    "Unit",1_500_000,5_000_000),
        ("Grease Pelumas 1kg",     "Kg",     80_000,  250_000),
        ("Inverter 2.2kW",         "Unit",  800_000,3_000_000),
    ],
    "Bahan Promosi & Cetak": [
        ("Banner Vinyl 3x1m",      "Pcs",   150_000,  450_000),
        ("Kaos Seragam Polo",      "Pcs",    80_000,  250_000),
        ("Topi Promosi",           "Pcs",    40_000,  150_000),
        ("Tas Goodie Bag",         "Pcs",    25_000,   90_000),
        ("Brosur Cetak 4/4",       "Rim",   200_000,  600_000),
        ("Stiker Logo (A4)",       "Lembar",  5_000,   20_000),
        ("Kalender Meja",          "Pcs",    25_000,   80_000),
        ("Payung Promosi",         "Pcs",    50_000,  200_000),
        ("Notebook Promosi",       "Pcs",    30_000,  120_000),
        ("Pulpen Promosi",         "Lusin",  60_000,  200_000),
    ],
}

# ---------------------------------------------------------------------------
# FUNGSI UTILITAS
# ---------------------------------------------------------------------------

def rnd_date(start: datetime, end: datetime) -> datetime:
    """Pilih tanggal acak antara start dan end."""
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))


def fmt_code(prefix: str, number: int, width: int = 5) -> str:
    return f"{prefix}{str(number).zfill(width)}"


def npwp_fake() -> str:
    """Generate format NPWP: XX.XXX.XXX.X-XXX.XXX"""
    return (
        f"{random.randint(10,99)}."
        f"{random.randint(100,999)}."
        f"{random.randint(100,999)}."
        f"{random.randint(1,9)}-"
        f"{random.randint(100,999)}."
        f"{random.randint(100,999)}"
    )


# ---------------------------------------------------------------------------
# 1. DEPARTMENTS
# ---------------------------------------------------------------------------

def generate_departments() -> pd.DataFrame:
    records = []
    for i, name in enumerate(DEPARTMENT_NAMES[:N_DEPARTMENTS], start=1):
        records.append({
            "department_id"   : i,
            "department_code" : fmt_code("DEPT", i, 3),
            "department_name" : name,
            "cost_center"     : f"CC{i * 100:06d}",
            "location"        : fake.city(),
            "manager_name"    : fake.name(),
            "budget_annual"   : round(random.uniform(500_000_000, 10_000_000_000)),
            "created_at"      : fake.date_between(start_date="-5y", end_date="-2y"),
        })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 2. VENDORS
# ---------------------------------------------------------------------------

def generate_vendors() -> pd.DataFrame:
    vendor_categories = list(ITEM_CATALOG.keys()) + [
        "Jasa Konsultasi", "Jasa Konstruksi", "Jasa Transportasi & Logistik",
        "Jasa Maintenance", "Bahan Kimia & Farmasi",
    ]
    payment_terms_opts = ["Net 15", "Net 30", "Net 45", "Net 60", "COD", "DP 50%"]
    vendor_type_opts   = ["Barang", "Jasa", "Barang & Jasa"]

    records = []
    for i in range(1, N_VENDORS + 1):
        records.append({
            "vendor_id"       : i,
            "vendor_code"     : fmt_code("VND", i, 4),
            "vendor_name"     : f"PT {fake.company()}",
            "vendor_category" : random.choice(vendor_categories),
            "vendor_type"     : random.choice(vendor_type_opts),
            "address"         : fake.address().replace("\n", ", "),
            "city"            : fake.city(),
            "province"        : random.choice(INDONESIA_PROVINCES),
            "postal_code"     : fake.postcode(),
            "phone"           : fake.phone_number(),
            "email"           : fake.company_email(),
            "npwp"            : npwp_fake(),
            "payment_terms"   : random.choice(payment_terms_opts),
            "rating"          : round(random.uniform(2.5, 5.0), 1),
            "is_active"       : random.choices([True, False], weights=[92, 8])[0],
            "registered_date" : fake.date_between(start_date="-8y", end_date="-1y"),
            "created_at"      : fake.date_between(start_date="-8y", end_date="-1y"),
        })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 3. ITEMS
# ---------------------------------------------------------------------------

def generate_items() -> pd.DataFrame:
    uom_pool = ["Pcs", "Unit", "Box", "Rim", "Lusin", "Set", "Roll",
                "Liter", "Galon", "Meter", "Kg", "Pack", "Lembar", "Botol", "Pasang"]
    records  = []
    item_id  = 1

    # Masukkan item dari katalog terlebih dahulu
    for category, catalog_items in ITEM_CATALOG.items():
        for (name, uom, price_min, price_max) in catalog_items:
            records.append({
                "item_id"          : item_id,
                "item_code"        : fmt_code("ITM", item_id, 4),
                "item_name"        : name,
                "item_category"    : category,
                "unit_of_measure"  : uom,
                "unit_price"       : round(random.uniform(price_min, price_max)),
                "min_order_qty"    : random.choice([1, 2, 5, 10, 12, 24]),
                "lead_time_days"   : random.randint(1, 14),
                "is_active"        : True,
                "created_at"       : fake.date_between(start_date="-5y", end_date="-2y"),
            })
            item_id += 1

    # Isi sisa item hingga N_ITEMS dengan variasi nama
    adjectives = ["Premium", "Standar", "Economy", "Pro", "Industrial", "Heavy Duty", "Lite"]
    while item_id <= N_ITEMS:
        category = random.choice(list(ITEM_CATALOG.keys()))
        base_item = random.choice(ITEM_CATALOG[category])
        base_name, uom, price_min, price_max = base_item
        adj = random.choice(adjectives)
        records.append({
            "item_id"          : item_id,
            "item_code"        : fmt_code("ITM", item_id, 4),
            "item_name"        : f"{base_name} ({adj})",
            "item_category"    : category,
            "unit_of_measure"  : uom,
            "unit_price"       : round(random.uniform(price_min * 0.9, price_max * 1.1)),
            "min_order_qty"    : random.choice([1, 2, 5, 10, 12, 24]),
            "lead_time_days"   : random.randint(1, 14),
            "is_active"        : random.choices([True, False], weights=[95, 5])[0],
            "created_at"       : fake.date_between(start_date="-5y", end_date="-2y"),
        })
        item_id += 1

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 4. PURCHASE REQUESTS
# ---------------------------------------------------------------------------

def generate_purchase_requests(dept_df: pd.DataFrame) -> pd.DataFrame:
    dept_ids    = dept_df["department_id"].tolist()
    pr_statuses = ["Disetujui", "Disetujui", "Disetujui", "Ditolak", "Pending Approval"]
    pr_weights  = [50, 20, 15, 10, 5]
    purposes    = [
        "Kebutuhan Operasional Rutin", "Proyek Khusus",
        "Penggantian Peralatan Rusak", "Penambahan Kapasitas Produksi",
        "Kebutuhan Darurat / Mendesak", "Pemeliharaan Rutin Bulanan",
        "Pengadaan Fasilitas Baru", "Renovasi & Perbaikan Gedung",
    ]
    priorities  = ["Rendah", "Sedang", "Tinggi", "Mendesak"]
    p_weights   = [20, 45, 25, 10]

    records = []
    for i in range(1, N_PR + 1):
        pr_date = rnd_date(DATE_START, DATE_END - timedelta(days=90))
        status  = random.choices(pr_statuses, weights=pr_weights)[0]
        approved = status in ("Disetujui",)
        records.append({
            "pr_id"         : i,
            "pr_number"     : f"PR-{pr_date.strftime('%Y%m')}-{i:05d}",
            "pr_date"       : pr_date.date(),
            "department_id" : random.choice(dept_ids),
            "requester_name": fake.name(),
            "requester_email": fake.email(),
            "purpose"       : random.choice(purposes),
            "priority"      : random.choices(priorities, weights=p_weights)[0],
            "status"        : status,
            "approved_by"   : fake.name() if approved else None,
            "approval_date" : (pr_date + timedelta(days=random.randint(1, 7))).date()
                              if approved else None,
            "total_amount"  : None,   # diupdate setelah PO Lines dibuat
            "notes"         : fake.sentence(nb_words=8) if random.random() > 0.55 else None,
            "created_at"    : pr_date.date(),
        })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 5. PURCHASE ORDERS
# ---------------------------------------------------------------------------

def generate_purchase_orders(
    pr_df: pd.DataFrame,
    vendor_df: pd.DataFrame,
    dept_df: pd.DataFrame,
) -> pd.DataFrame:

    # Hanya PR yang disetujui bisa menghasilkan PO
    approved_pr = pr_df[pr_df["status"] == "Disetujui"].copy()
    approved_pr["pr_date"] = pd.to_datetime(approved_pr["pr_date"])

    active_vendor_ids = vendor_df[vendor_df["is_active"]]["vendor_id"].tolist()
    dept_ids          = dept_df["department_id"].tolist()

    po_statuses = ["Terbuka", "Sebagian Diterima", "Selesai", "Dibatalkan"]
    po_weights  = [15, 20, 55, 10]
    pay_terms   = ["Net 15", "Net 30", "Net 45", "Net 60", "COD", "DP 50%"]

    # Ambil N_PO PR secara acak (dengan replacement jika kurang)
    n_sample = min(N_PO, len(approved_pr))
    sampled_pr = approved_pr.sample(n=n_sample, replace=(N_PO > n_sample),
                                    random_state=42).reset_index(drop=True)

    records = []
    for i, row in sampled_pr.iterrows():
        po_id    = i + 1
        pr_date  = row["pr_date"]
        po_date  = pr_date + timedelta(days=random.randint(3, 14))
        if po_date.date() > DATE_END.date():
            po_date = datetime(DATE_END.year, DATE_END.month, 1)

        exp_delivery = po_date + timedelta(days=random.randint(3, 30))
        status       = random.choices(po_statuses, weights=po_weights)[0]

        records.append({
            "po_id"                  : po_id,
            "po_number"              : f"PO-{po_date.strftime('%Y%m')}-{po_id:05d}",
            "po_date"                : po_date.date(),
            "pr_id"                  : int(row["pr_id"]),
            "vendor_id"              : random.choice(active_vendor_ids),
            "department_id"          : int(row["department_id"]),
            "expected_delivery_date" : exp_delivery.date(),
            "payment_terms"          : random.choice(pay_terms),
            "status"                 : status,
            "shipping_address"       : fake.address().replace("\n", ", "),
            "approved_by"            : fake.name(),
            "approval_date"          : (po_date + timedelta(days=random.randint(0, 3))).date(),
            "total_amount"           : None,   # diupdate setelah PO Lines dibuat
            "notes"                  : fake.sentence(nb_words=6) if random.random() > 0.6 else None,
            "created_at"             : po_date.date(),
        })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 6. PURCHASE ORDER LINES
# ---------------------------------------------------------------------------

def generate_po_lines(po_df: pd.DataFrame, items_df: pd.DataFrame) -> pd.DataFrame:
    active_items = items_df[items_df["is_active"]].copy().reset_index(drop=True)
    n_items      = len(active_items)

    qty_choices  = [1, 2, 3, 5, 10, 12, 20, 24, 50, 100]

    records  = []
    line_id  = 1

    for _, po in po_df.iterrows():
        # Jumlah baris per PO: distribusi realistis (sebagian besar 3-7 baris)
        n_lines = random.choices(
            [2, 3, 4, 5, 6, 7, 8, 10],
            weights=[5, 15, 20, 25, 15, 10, 7, 3],
        )[0]
        n_lines = min(n_lines, n_items)

        # Pilih item tanpa duplikat dalam satu PO
        chosen_indices = random.sample(range(n_items), n_lines)

        for line_num, idx in enumerate(chosen_indices, start=1):
            item      = active_items.iloc[idx]
            base_price = float(item["unit_price"])

            # Negosiasi harga: vendor bisa memberi diskon atau premium kecil
            discount_pct = round(random.choices(
                [0, 0, 0, 5, 10, 15],
                weights=[40, 25, 15, 10, 7, 3],
            )[0], 1)
            unit_price   = round(base_price * (1 - discount_pct / 100))

            qty_mult      = random.choice(qty_choices)
            qty_ordered   = qty_mult * int(item["min_order_qty"])
            total_price   = unit_price * qty_ordered

            records.append({
                "line_id"               : line_id,
                "po_id"                 : int(po["po_id"]),
                "item_id"               : int(item["item_id"]),
                "line_number"           : line_num,
                "quantity_ordered"      : qty_ordered,
                "unit_of_measure"       : item["unit_of_measure"],
                "unit_price"            : unit_price,
                "discount_pct"          : discount_pct,
                "total_price"           : total_price,
                "delivery_date_expected": po["expected_delivery_date"],
                "line_status"           : po["status"],
                "notes"                 : fake.sentence(nb_words=5) if random.random() > 0.8 else None,
                "created_at"            : po["po_date"],
            })
            line_id += 1

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 7. GOODS RECEIPTS
# ---------------------------------------------------------------------------

def generate_goods_receipts(
    po_lines_df: pd.DataFrame,
    po_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Skenario analitik:
    ──────────────────
    • Lead time realistis: GR date = PO date + 3 s/d 30 hari (distribusi log-normal)
      dengan kemungkinan terlambat (GR date > expected_delivery_date).
    • Variasi kuantitas:
        - Lengkap     (55 %) : qty_received == qty_ordered
        - Kurang Ringan(20 %) : 90–99 % dari qty_ordered   → status "Kurang"
        - Kurang Sedang(15 %) : 75–89 % dari qty_ordered   → status "Kurang Signifikan"
        - Kurang Parah  (7 %) : 50–74 % dari qty_ordered   → status "Barang Sangat Kurang"
        - Lebih        ( 3 %) : 101–115 % dari qty_ordered → status "Lebih"
    • Hanya PO berstatus "Selesai" atau "Sebagian Diterima" yang memiliki GR.
    """
    receivable_statuses = {"Selesai", "Sebagian Diterima"}
    receivable_po_ids   = set(po_df.loc[po_df["status"].isin(receivable_statuses), "po_id"])

    # Buat lookup po_date & expected_delivery dari po_df
    po_lookup = po_df.set_index("po_id")[["po_date", "expected_delivery_date"]].copy()
    po_lookup["po_date"]                = pd.to_datetime(po_lookup["po_date"])
    po_lookup["expected_delivery_date"] = pd.to_datetime(po_lookup["expected_delivery_date"])

    recv_lines = po_lines_df[po_lines_df["po_id"].isin(receivable_po_ids)].copy()

    warehouse_locs = [
        "WH-A01", "WH-A02", "WH-A03",
        "WH-B01", "WH-B02",
        "WH-C01", "WH-C02",
        "WH-D01",
    ]
    inspection_opts     = ["Lulus", "Lulus", "Lulus", "Perlu Pemeriksaan Ulang", "Ditolak"]
    inspection_weights  = [70, 0, 0, 20, 10]   # hanya 3 opsi aktif, di-normalize di bawah

    scenario_opts    = ["lengkap", "kurang_ringan", "kurang_sedang", "kurang_parah", "lebih"]
    scenario_weights = [55, 20, 15, 7, 3]

    records = []
    gr_id   = 1

    for _, line in recv_lines.iterrows():
        po_id    = int(line["po_id"])
        po_info  = po_lookup.loc[po_id]
        po_date  = po_info["po_date"]
        exp_del  = po_info["expected_delivery_date"]

        # ── Lead time ────────────────────────────────────────────────────────
        # Distribusi: sebagian besar on-time (lead 3–30 hari), sebagian terlambat
        on_time = random.random() < 0.72   # 72 % pengiriman on-time

        if on_time:
            lead_days = int(np.clip(np.random.lognormal(mean=2.5, sigma=0.5), 3, 30))
            gr_date   = po_date + timedelta(days=lead_days)
            # Pastikan tidak melebihi expected_delivery jika on-time
            if gr_date > exp_del:
                gr_date = exp_del - timedelta(days=random.randint(0, 2))
        else:
            # Terlambat: 1–45 hari setelah expected_delivery
            delay     = random.randint(1, 45)
            gr_date   = exp_del + timedelta(days=delay)

        # Batas atas: tidak boleh melebihi DATE_END
        if gr_date.date() > DATE_END.date():
            gr_date = datetime.combine(DATE_END.date(), datetime.min.time())

        lead_time_actual = (gr_date - po_date).days
        is_on_time       = gr_date.date() <= exp_del.date()

        # ── Variasi kuantitas ─────────────────────────────────────────────────
        qty_ordered = int(line["quantity_ordered"])
        scenario    = random.choices(scenario_opts, weights=scenario_weights)[0]

        if scenario == "lengkap":
            qty_received = qty_ordered
        elif scenario == "kurang_ringan":
            qty_received = max(1, round(qty_ordered * random.uniform(0.90, 0.99)))
        elif scenario == "kurang_sedang":
            qty_received = max(1, round(qty_ordered * random.uniform(0.75, 0.89)))
        elif scenario == "kurang_parah":
            qty_received = max(1, round(qty_ordered * random.uniform(0.50, 0.74)))
        else:  # lebih
            qty_received = round(qty_ordered * random.uniform(1.01, 1.15))

        qty_variance   = qty_received - qty_ordered
        variance_pct   = round((qty_variance / qty_ordered) * 100, 2)

        # Status penerimaan
        if qty_variance == 0:
            receipt_status = "Lengkap"
        elif qty_variance > 0:
            receipt_status = "Lebih"
        elif abs(qty_variance) / qty_ordered <= 0.10:
            receipt_status = "Kurang"
        elif abs(qty_variance) / qty_ordered <= 0.25:
            receipt_status = "Kurang Signifikan"
        else:
            receipt_status = "Barang Sangat Kurang"

        unit_price           = float(line["unit_price"])
        total_received_value = round(qty_received * unit_price)

        records.append({
            "gr_id"               : gr_id,
            "gr_number"           : f"GR-{gr_date.strftime('%Y%m')}-{gr_id:06d}",
            "gr_date"             : gr_date.date(),
            "po_id"               : po_id,
            "po_line_id"          : int(line["line_id"]),
            "item_id"             : int(line["item_id"]),
            "quantity_ordered"    : qty_ordered,
            "quantity_received"   : qty_received,
            "quantity_variance"   : qty_variance,
            "variance_pct"        : variance_pct,
            "receipt_status"      : receipt_status,
            "lead_time_days"      : lead_time_actual,
            "is_on_time"          : is_on_time,
            "unit_price"          : unit_price,
            "total_received_value": total_received_value,
            "warehouse_location"  : random.choice(warehouse_locs),
            "received_by"         : fake.name(),
            "inspection_status"   : random.choices(
                                        ["Lulus", "Perlu Pemeriksaan Ulang", "Ditolak"],
                                        weights=[75, 18, 7],
                                    )[0],
            "notes"               : fake.sentence(nb_words=6) if random.random() > 0.70 else None,
            "created_at"          : gr_date.date(),
        })
        gr_id += 1

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# UPDATE TOTAL AMOUNT (denormalisasi untuk kemudahan analitik)
# ---------------------------------------------------------------------------

def update_totals(
    pr_df: pd.DataFrame,
    po_df: pd.DataFrame,
    po_lines_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Isi kolom total_amount di tabel PO dan PR berdasarkan PO Lines."""

    # PO total
    po_totals = (
        po_lines_df.groupby("po_id")["total_price"]
        .sum()
        .reset_index()
        .rename(columns={"total_price": "total_amount"})
    )
    po_df = po_df.drop(columns=["total_amount"]).merge(po_totals, on="po_id", how="left")
    po_df["total_amount"] = po_df["total_amount"].fillna(0).astype(int)

    # PR total: jumlahkan semua PO yang terkait
    pr_totals = (
        po_df.groupby("pr_id")["total_amount"]
        .sum()
        .reset_index()
        .rename(columns={"total_amount": "total_amount"})
    )
    pr_df = pr_df.drop(columns=["total_amount"]).merge(pr_totals, on="pr_id", how="left")
    pr_df["total_amount"] = pr_df["total_amount"].fillna(0).astype(int)

    return pr_df, po_df


# ---------------------------------------------------------------------------
# EKSPOR CSV
# ---------------------------------------------------------------------------

def export_csv(tables: dict[str, pd.DataFrame]) -> None:
    print("\n📁 Ekspor ke CSV ...")
    for name, df in tables.items():
        filepath = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        size_kb = os.path.getsize(filepath) / 1024
        print(f"   ✓ {name}.csv  →  {len(df):>7,} baris | {len(df.columns):>2} kolom | {size_kb:>8.1f} KB")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 65)
    print("  Procurement Synthetic Data Generator  |  Faker id_ID")
    print("=" * 65)

    print("\n[1/7] Membuat tabel departments ...")
    dept_df = generate_departments()
    print(f"      → {len(dept_df):,} baris")

    print("[2/7] Membuat tabel vendors ...")
    vendor_df = generate_vendors()
    print(f"      → {len(vendor_df):,} baris")

    print("[3/7] Membuat tabel items ...")
    items_df = generate_items()
    print(f"      → {len(items_df):,} baris")

    print("[4/7] Membuat tabel purchase_requests ...")
    pr_df = generate_purchase_requests(dept_df)
    print(f"      → {len(pr_df):,} baris")

    print("[5/7] Membuat tabel purchase_orders ...")
    po_df = generate_purchase_orders(pr_df, vendor_df, dept_df)
    print(f"      → {len(po_df):,} baris")

    print("[6/7] Membuat tabel purchase_order_lines ...")
    po_lines_df = generate_po_lines(po_df, items_df)
    print(f"      → {len(po_lines_df):,} baris  ✓ (syarat ≥10.000)")

    print("[7/7] Membuat tabel goods_receipts ...")
    gr_df = generate_goods_receipts(po_lines_df, po_df)
    print(f"      → {len(gr_df):,} baris")

    # Update total_amount
    pr_df, po_df = update_totals(pr_df, po_df, po_lines_df)

    tables = {
        "departments"         : dept_df,
        "vendors"             : vendor_df,
        "items"               : items_df,
        "purchase_requests"   : pr_df,
        "purchase_orders"     : po_df,
        "purchase_order_lines": po_lines_df,
        "goods_receipts"      : gr_df,
    }

    export_csv(tables)

    # ── Ringkasan ──────────────────────────────────────────────────────────
    total_rows = sum(len(df) for df in tables.values())
    print("\n" + "=" * 65)
    print("  RINGKASAN AKHIR")
    print("=" * 65)
    print(f"  Total baris seluruh tabel : {total_rows:,}")
    print(f"  Tabel transaksi utama     : purchase_order_lines = {len(po_lines_df):,} baris")
    print(f"  Goods Receipts            : {len(gr_df):,} baris")
    print(f"  Output directory          : {os.path.abspath(OUTPUT_DIR)}")

    # Statistik analitik sedang
    print("\n  ── Distribusi Status PO ──")
    print(po_df["status"].value_counts().to_string())

    print("\n  ── Distribusi Status Penerimaan GR ──")
    print(gr_df["receipt_status"].value_counts().to_string())

    print("\n  ── Statistik Lead Time (hari) ──")
    lt = gr_df["lead_time_days"]
    print(f"  Min: {lt.min()}  |  Median: {lt.median():.0f}  |  Mean: {lt.mean():.1f}  |  Max: {lt.max()}")
    print(f"  On-time delivery rate: {gr_df['is_on_time'].mean() * 100:.1f}%")

    print("\n  ── Statistik Variance Qty (%) ──")
    vp = gr_df["variance_pct"]
    print(f"  Min: {vp.min():.1f}%  |  Mean: {vp.mean():.2f}%  |  Max: {vp.max():.1f}%")
    print("=" * 65)
    print("  Selesai! 🎉")
    print("=" * 65)


if __name__ == "__main__":
    main()
