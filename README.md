# 🌍 Geo-Analysis — GEE Pipeline

ระบบ Pipeline อัตโนมัติสำหรับประมวลผลข้อมูลภูมิสารสนเทศ โดยใช้ **Google Earth Engine**, **Python** และ **GitHub Actions**

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Google Earth Engine](https://img.shields.io/badge/Google%20Earth%20Engine-4285F4?style=flat-square&logo=google&logoColor=white)](https://earthengine.google.com)
[![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?style=flat-square&logo=github-actions&logoColor=white)](https://github.com/features/actions)

---

## Pipeline โดยรวม

```
Extract (GEE)  →  Clean Data  →  Impute Missing  →  DTW Anomaly
```

ดึงข้อมูลดาวเทียม → ทำความสะอาด → เติมค่าที่หายไป → ตรวจจับความผิดปกติ

---

## 📁 โครงสร้างโปรเจกต์

```
geo-analysis/
├── .github/workflows/
│   ├── monthly-run.yml          # Main pipeline (Extract → Clean → Impute)
│   ├── dtw-run.yml              # คำนวณ DTW Anomaly
│   └── monthly-raw-ingest.yml   # อัปเดตข้อมูลดิบรายเดือน
│
├── config/
│   └── .env                     # Environment Variables
│
├── scripts/                     # Python scripts หลัก
│   ├── export.py                # ดึงข้อมูลจาก GEE
│   ├── clean.py                 # ทำความสะอาดข้อมูล
│   └── merge.py                 # รวมข้อมูล
│
├── scripts_auto/                # สคริปต์สำหรับระบบอัตโนมัติ
│
├── outputs/
│   ├── imputed/                 # ข้อมูลที่เติมค่าแล้ว
│   ├── merged/                  # ข้อมูลที่ Merge แล้ว
│   ├── dtw_subdistrict.csv      # DTW ระดับตำบล
│   ├── dtw_district.csv         # DTW ระดับอำเภอ
│   └── dtw_province.csv         # DTW ระดับจังหวัด
│
└── requirements.txt
```

---

## ⚙️ ตั้งค่า Environment

สร้างไฟล์ `config/.env` แล้วกำหนดค่าดังนี้:

```env
SERVICE_ACCOUNT=gee-runner@geo-analysis-472713.iam.gserviceaccount.com
GCS_BUCKET=geo-analysis-storage
GOOGLE_APPLICATION_CREDENTIALS=gee-pipeline/service-key.json
```

> [!IMPORTANT]
> อย่า commit ไฟล์ `.env` หรือ Service Account Key ขึ้น GitHub
> ใช้ **GitHub Secrets** แทนสำหรับ Workflow อัตโนมัติ

---

## 🚀 Workflows

### `monthly-run.yml` — Main Pipeline
รันอัตโนมัติทุกเดือน ครอบคลุมทุกขั้นตอนตั้งแต่ดึงข้อมูลจนถึง Imputation

### `dtw-run.yml` — DTW Anomaly Detection
คำนวณ Dynamic Time Warping เพื่อตรวจจับความผิดปกติใน 3 ระดับ ได้แก่ ตำบล อำเภอ และจังหวัด

### `monthly-raw-ingest.yml` — Raw Data Ingestion
อัปเดตข้อมูลดิบอัตโนมัติ รองรับการขยายผลในอนาคต

---

## 🛠️ การติดตั้ง

**1. Clone repository**
```bash
git clone https://github.com/<your-username>/geo-analysis.git
cd geo-analysis
```

**2. ติดตั้ง dependencies**
```bash
pip install -r requirements.txt
```

**3. ตั้งค่า GEE Authentication**
```bash
# สำหรับใช้งานบนเครื่องตัวเอง
earthengine authenticate
```

```python
# สำหรับ Automated Pipeline ใช้ Service Account
import ee
credentials = ee.ServiceAccountCredentials(
    email=SERVICE_ACCOUNT,
    key_file=GOOGLE_APPLICATION_CREDENTIALS
)
ee.Initialize(credentials)
```

**4. รัน Pipeline แบบ Manual**
```bash
python scripts/export.py   # ดึงข้อมูลจาก GEE
python scripts/clean.py    # ทำความสะอาดข้อมูล
python scripts/merge.py    # Merge ข้อมูล
```

### Libraries ที่ใช้

| Library | หน้าที่ |
|---------|--------|
| `earthengine-api` | เชื่อมต่อ Google Earth Engine |
| `google-cloud-storage` | จัดการไฟล์บน GCS |
| `pandas`, `geopandas` | จัดการข้อมูลตารางและเชิงพื้นที่ |
| `scipy`, `numpy` | คำนวณ DTW และสถิติ |

---

## 📊 Output

ผลลัพธ์จะอยู่ในโฟลเดอร์ `outputs/` แบ่งตามระดับการปกครอง:

| ไฟล์ | ระดับ |
|------|-------|
| `dtw_subdistrict.csv` | ตำบล |
| `dtw_district.csv` | อำเภอ |
| `dtw_province.csv` | จังหวัด |

นำข้อมูลไปใช้ต่อได้ใน GIS tools (QGIS, Kepler.gl), Dashboard (Grafana, Power BI) หรือ ML Pipeline ขั้นต่อไป
