# DTW Result Dataset Documentation

ไฟล์นี้อธิบายโครงสร้างและความหมายของทุก column  
ในไฟล์ผลลัพธ์ `dtw_results.parquet`  
ซึ่งได้จากการคำนวณ **Dynamic Time Warping (DTW)**  
เพื่อวิเคราะห์ความผิดปกติของข้อมูลเชิงเวลา (รายเดือน 12 เดือน)  
ในระดับ **ตำบล (Subdistrict)**



## 1. Dataset Overview

- **ระดับข้อมูล**: ตำบล × ปี  
- **Temporal Resolution**: รายเดือน (12 เดือนต่อปี)
- **Spatial Resolution**: Province → District → Subdistrict
- **Baseline**: ค่าเฉลี่ยแบบ Trimmed Mean (10%) รายเดือน ของแต่ละตำบล
- **Normalization**: Z-score ต่อพื้นที่
- **Threshold**:  
  - Local threshold (mean + 2σ)  
  - Global threshold (Z-score > 2)



## 2. Spatial & Temporal Identifiers

| Column | Description |
|------|------------|
| `province` | ชื่อจังหวัด |
| `district` | ชื่ออำเภอ |
| `subdistrict` | ชื่อตำบล |
| `year` | ปีที่ทำการคำนวณ DTW |

> 1 แถว แทนข้อมูลของ **ตำบลหนึ่งแห่งในหนึ่งปี**



## 3. Raw DTW Distance Columns

DTW วัดความแตกต่างของ **รูปแบบข้อมูลรายเดือน (12 เดือน)**  
ระหว่างปีที่พิจารณา กับ baseline ปกติของตำบลเดียวกัน

| Column | Meaning |
|------|--------|
| `dtw_ndvi` | ความผิดปกติของรูปแบบ NDVI |
| `dtw_rainfall` | ความผิดปกติของรูปแบบปริมาณฝน |
| `dtw_soilmoisture` | ความผิดปกติของรูปแบบความชื้นในดิน |
| `dtw_lst` | ความผิดปกติของรูปแบบอุณหภูมิพื้นผิว (LST) |
| `dtw_firecount` | ความผิดปกติของรูปแบบจำนวนจุดความร้อน |

**Interpretation**
- ค่า DTW ต่ำ → รูปแบบคล้ายพฤติกรรมปกติ
- ค่า DTW สูง → รูปแบบเบี่ยงเบนจากพฤติกรรมปกติ

> ค่า DTW เป็นค่าที่ไม่มีหน่วยและไม่สามารถเปรียบเทียบข้ามพื้นที่ได้โดยตรง  
จึงต้องมีการ normalize ในขั้นตอนถัดไป



## 4. Local DTW Statistics (Per Subdistrict)

คำนวณจากค่า DTW ของตำบลเดียวกันทุกปี

| Column | Description |
|------|------------|
| `dtw_xxx_local_mean` | ค่าเฉลี่ย DTW ของตัวแปรนั้นในตำบล |
| `dtw_xxx_local_std` | ส่วนเบี่ยงเบนมาตรฐานของ DTW ในตำบล |

ตัวอย่าง:
- `dtw_ndvi_local_mean`
- `dtw_ndvi_local_std`

> สถิติเหล่านี้สะท้อน “พฤติกรรมปกติ” และระดับความผันผวนของแต่ละพื้นที่



## 5. Normalized DTW (Z-score)

| Column | Description |
|------|------------|
| `dtw_xxx_z` | ค่า DTW หลัง normalize ด้วย Z-score ต่อพื้นที่ |

**Z = (DTW - local_mean) / local_std**



**Interpretation**
- Z ≈ 0 → ปกติ
- Z > 2 → ผิดปกติสูง
- Z < 0 → ต่ำกว่าค่าเฉลี่ย

> หลัง normalize แล้ว ค่า DTW สามารถเปรียบเทียบข้ามพื้นที่ได้



## 6. Local Threshold & Anomaly Flag

Threshold แบบเฉพาะพื้นที่  
คำนวณจากสถิติของตำบลนั้นเอง

| Column | Description |
|------|------------|
| `dtw_xxx_local_threshold` | ค่า threshold (mean + 2σ) |
| `dtw_xxx_flag` | 1 = ผิดปกติ, 0 = ปกติ |

**Use case**
- การเฝ้าระวังภัยเฉพาะพื้นที่
- Early warning ระดับตำบล



## 7. Global Threshold (Z-score Based)

| Column | Description |
|------|------------|
| `dtw_xxx_z_flag` | 1 = Z-score > 2, 0 = ปกติ |

**Interpretation**
- ใช้ threshold เดียวกันทุกพื้นที่
- เหมาะสำหรับการเปรียบเทียบเชิงพื้นที่และการทำ hotspot map



## 8. Summary Concept

- **DTW**: วัดความผิดปกติของรูปแบบเชิงเวลา
- **Local statistics**: สะท้อนพฤติกรรมปกติของแต่ละพื้นที่
- **Z-score**: ทำให้ข้อมูลอยู่บนสเกลเดียวกัน
- **Local threshold**: ตรวจจับ anomaly เฉพาะพื้นที่
- **Global threshold**: เปรียบเทียบความรุนแรงข้ามพื้นที่



## 9. Recommended Usage

- ใช้ `dtw_xxx_z` สำหรับ visualization ข้ามพื้นที่
- ใช้ `dtw_xxx_flag` สำหรับ early warning รายพื้นที่
- ใช้ `dtw_xxx_z_flag` สำหรับการระบุ hotspot ระดับภูมิภาค
- สามารถรวมหลายตัวแปรเพื่อสร้าง composite anomaly index ได้

---
