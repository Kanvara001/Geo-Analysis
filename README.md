<div align="center">

<br/>

```
   ██████╗ ███████╗ ██████╗       █████╗ ███╗   ██╗ █████╗ ██╗  ██╗   ██╗███████╗██╗███████╗
  ██╔════╝ ██╔════╝██╔═══██╗     ██╔══██╗████╗  ██║██╔══██╗██║  ╚██╗ ██╔╝██╔════╝██║██╔════╝
  ██║  ███╗█████╗  ██║   ██║     ███████║██╔██╗ ██║███████║██║   ╚████╔╝ ███████╗██║███████╗
  ██║   ██║██╔══╝  ██║   ██║     ██╔══██║██║╚██╗██║██╔══██║██║    ╚██╔╝  ╚════██║██║╚════██║
  ╚██████╔╝███████╗╚██████╔╝     ██║  ██║██║ ╚████║██║  ██║███████╗██║   ███████║██║███████║
   ╚═════╝ ╚══════╝ ╚═════╝      ╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝╚═╝   ╚══════╝╚═╝╚══════╝
```

**Geospatial Analysis Pipeline — Powered by Google Earth Engine**

<br/>

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Google Earth Engine](https://img.shields.io/badge/Google%20Earth%20Engine-4285F4?style=flat-square&logo=google&logoColor=white)](https://earthengine.google.com)
[![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?style=flat-square&logo=github-actions&logoColor=white)](https://github.com/features/actions)
[![License](https://img.shields.io/badge/License-MIT-22c55e?style=flat-square)](LICENSE)

<br/>

> ระบบ Pipeline อัตโนมัติสำหรับประมวลผลข้อมูลภูมิสารสนเทศ  
> ดึงข้อมูล · ทำความสะอาด · เติมข้อมูลที่ขาดหาย · คำนวณ DTW Anomaly

<br/>

</div>

---

## 📋 สารบัญ

- [ภาพรวมโปรเจกต์](#-ภาพรวมโปรเจกต์)
- [โครงสร้างโฟลเดอร์](#-โครงสร้างโฟลเดอร์)
- [การตั้งค่าระบบ](#️-การตั้งค่าระบบ)
- [ระบบการทำงานอัตโนมัติ](#-ระบบการทำงานอัตโนมัติ)
- [การติดตั้งและใช้งาน](#️-การติดตั้งและใช้งาน)
- [ข้อมูลผลลัพธ์](#-ข้อมูลผลลัพธ์)

---

## 🌍 ภาพรวมโปรเจกต์

**Geo-Analysis** คือระบบ Pipeline สำหรับประมวลผลข้อมูลทางภูมิสารสนเทศ (Geospatial Data) แบบอัตโนมัติ โดยผสานการทำงานระหว่าง **Google Earth Engine (GEE)**, **Python** และ **GitHub Actions** เข้าด้วยกัน

```
┌─────────────────────────────────────────────────────────────────┐
│                       GEE PIPELINE FLOW                         │
│                                                                 │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌─────────┐  │
│   │  Extract │───▶│  Clean   │───▶│  Impute  │───▶│   DTW   │  │
│   │  (GEE)   │    │  Data    │    │  Missing │    │ Anomaly │  │
│   └──────────┘    └──────────┘    └──────────┘    └─────────┘  │
│        │               │               │               │        │
│    Raw Satellite    Remove Noise    Fill Gaps       Detect      │
│       Data          & Outliers      in Series      Patterns     │
└─────────────────────────────────────────────────────────────────┘
```

### ความสามารถหลัก

| ฟีเจอร์ | รายละเอียด |
|--------|-----------|
| 🛰️ **Data Extraction** | ดึงข้อมูลดาวเทียมจาก Google Earth Engine โดยตรง |
| 🧹 **Data Cleaning** | ทำความสะอาดและกำจัด Outlier จากข้อมูล Time Series |
| 🔧 **Imputation** | เติมข้อมูลที่ขาดหายด้วยเทคนิคเชิงสถิติ |
| 📊 **DTW Analysis** | คำนวณ Dynamic Time Warping เพื่อตรวจจับความผิดปกติ |
| ⚙️ **CI/CD Automation** | รันอัตโนมัติทุกเดือนผ่าน GitHub Actions |

---

## 📁 โครงสร้างโฟลเดอร์

```
geo-analysis/
│
├── 📂 .github/
│   └── workflows/
│       ├── monthly-run.yml         # Main workflow (Extract → Clean → Impute)
│       ├── dtw-run.yml             # DTW Anomaly calculation
│       └── monthly-raw-ingest.yml  # Raw data ingestion
│
├── 📂 config/
│   └── .env                        # Environment Variables
│
├── 📂 scripts/                     # Python scripts หลัก
│   ├── export.py                   # ดึงข้อมูลจาก GEE
│   ├── clean.py                    # ทำความสะอาดข้อมูล
│   └── merge.py                    # รวมและจัดการข้อมูล
│
├── 📂 scripts_auto/                # สคริปต์สำหรับระบบอัตโนมัติ
│   └── ...
│
├── 📂 outputs/                     # ผลลัพธ์การประมวลผล
│   ├── imputed/                    # ข้อมูลที่ผ่านการเติมค่า
│   ├── merged/                     # ข้อมูลที่ผ่านการ Merge
│   ├── dtw_subdistrict.csv         # DTW ระดับตำบล
│   ├── dtw_district.csv            # DTW ระดับอำเภอ
│   └── dtw_province.csv            # DTW ระดับจังหวัด
│
└── requirements.txt
```

---

## ⚙️ การตั้งค่าระบบ

สร้างไฟล์ `.env` ในโฟลเดอร์ `config/` หรือตั้งค่า Environment Variables ดังนี้:

```env
# Google Earth Engine Service Account
SERVICE_ACCOUNT=gee-runner@geo-analysis-472713.iam.gserviceaccount.com

# Google Cloud Storage Bucket
GCS_BUCKET=geo-analysis-storage

# Path to GEE Service Account Key
GOOGLE_APPLICATION_CREDENTIALS=gee-pipeline/service-key.json
```

> [!IMPORTANT]  
> ไม่ควร Commit ไฟล์ `.env` หรือ Service Account Key ขึ้น GitHub  
> ให้เพิ่มไฟล์เหล่านี้ใน `.gitignore` และใช้ **GitHub Secrets** แทนใน Workflow

---

## 🚀 ระบบการทำงานอัตโนมัติ

### `monthly-run.yml` — Main Pipeline

Workflow หลักที่เชื่อมทุกขั้นตอนเข้าด้วยกัน รันอัตโนมัติทุกเดือน

```
GEE Extract  ──▶  Data Clean  ──▶  Imputation  ──▶  Upload to GCS
```

### `dtw-run.yml` — DTW Anomaly Detection

คำนวณค่า **Dynamic Time Warping** เพื่อตรวจจับความผิดปกติของข้อมูลใน 3 ระดับพื้นที่:

- 🏘️ **Subdistrict** (ตำบล)
- 🏙️ **District** (อำเภอ)  
- 🗺️ **Province** (จังหวัด)

### `monthly-raw-ingest.yml` — Raw Data Ingestion

ระบบอัปเดตข้อมูลดิบอัตโนมัติ ออกแบบมาเพื่อรองรับการขยายผลและการเชื่อมต่อข้อมูลในอนาคต

---

## 🛠️ การติดตั้งและใช้งาน

### 1. Clone Repository

```bash
git clone https://github.com/<your-username>/geo-analysis.git
cd geo-analysis
```

### 2. ติดตั้ง Dependencies

```bash
pip install -r requirements.txt
```

### 3. Library หลักที่ใช้งาน

| Library | การใช้งาน |
|---------|----------|
| `earthengine-api` | เชื่อมต่อและดึงข้อมูลจาก Google Earth Engine |
| `google-cloud-storage` | จัดการข้อมูลบน GCS Bucket |
| `pandas` / `geopandas` | จัดการข้อมูลตารางและเชิงพื้นที่ |
| `scipy` / `numpy` | การคำนวณทางคณิตศาสตร์และ DTW |

### 4. ตั้งค่า GEE Authentication

```bash
earthengine authenticate
```

หรือใช้ Service Account สำหรับ Automated Pipeline:

```python
import ee

credentials = ee.ServiceAccountCredentials(
    email=SERVICE_ACCOUNT,
    key_file=GOOGLE_APPLICATION_CREDENTIALS
)
ee.Initialize(credentials)
```

### 5. รัน Pipeline แบบ Manual

```bash
# ดึงข้อมูลจาก GEE
python scripts/export.py

# ทำความสะอาดข้อมูล
python scripts/clean.py

# Merge ข้อมูล
python scripts/merge.py
```

---

## 📊 ข้อมูลผลลัพธ์

ข้อมูลที่ประมวลผลเสร็จสิ้นจะถูกเก็บไว้ในโฟลเดอร์ `outputs/` แบ่งออกเป็น 3 ระดับการปกครอง:

```
outputs/
├── dtw_subdistrict.csv   →  DTW Anomaly Score ระดับตำบล
├── dtw_district.csv      →  DTW Anomaly Score ระดับอำเภอ
└── dtw_province.csv      →  DTW Anomaly Score ระดับจังหวัด
```

ข้อมูลเหล่านี้พร้อมนำไปใช้งานใน:
- 🗺️ **GIS Visualization** (QGIS, ArcGIS, Kepler.gl)
- 📈 **Data Dashboard** (Grafana, Metabase, Power BI)
- 🤖 **Machine Learning Pipeline** ขั้นต่อไป

---

<div align="center">

<br/>

Made with ❤️ for Geospatial Research

</div>
