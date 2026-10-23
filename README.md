# ระบบรวมราคา CPU จากหลายแหล่ง (02739452 Final)

**นิสิต:** ธีรกานต์ · รหัส 6621600917 · ลำดับที่ 11

## หัวข้อ

- **หมวด:** ราคาสินค้าไอที
- **ขอบเขต:** ราคา CPU Intel Core รุ่นที่ 12–14, Intel Core Ultra (เดสก์ท็อป ซีรีส์ 2xx) และ AMD Ryzen เดสก์ท็อป ในประเทศไทย เทียบกับคะแนน benchmark

## แหล่งข้อมูล

ทั้ง 3 แหล่งเป็น web scraping (ไม่ใช้ Selenium)

| # | แหล่ง | วิธีดึง | ข้อมูลที่ได้ |
|---|---|---|---|
| 1 | [ihavecpu.com](https://ihavecpu.com/category/cpu) | `requests` + BeautifulSoup อ่าน JSON ใน `<script id="__NEXT_DATA__">` ของหน้า HTML (แบ่งหน้าด้วย `?page=N`) | ชื่อสินค้า, ราคาขาย (บาท) |
| 2 | [priceza.com](https://www.priceza.com) | scrape การ์ดสินค้าในหน้า `/s/ราคา/<คำค้น>` โดยใช้ **Dynamic Task Mapping** 1 task ต่อ 1 คำค้น (24 คำค้น) | ชื่อรุ่น, ช่วงราคาต่ำสุด–สูงสุดจากหลายร้าน, จำนวนร้าน |
| 3 | [cpubenchmark.net (PassMark)](https://www.cpubenchmark.net/high_end_cpus.html) | scrape หน้า chart high-end และ mid-range | คะแนน CPU Mark, ราคาอ้างอิง (USD) |

อัตราแลกเปลี่ยน USD→THB ดึงจาก open.er-api.com (ถ้าล่มจะใช้ค่า default และบันทึกไว้)

**จริยธรรมและ PDPA**
- ตรวจ `robots.txt` ก่อนทุก request และตั้ง User-Agent ระบุตัวตน
- เว้นช่วง 1.5 วินาทีต่อ request
- จำกัดให้ Priceza มี task ทำงานพร้อมกันไม่เกิน 2
- **ไม่เก็บชื่อร้านหรือผู้ขาย** หรือข้อมูลบุคคลใด ๆ (เก็บเฉพาะชื่อรุ่น ราคา และคะแนน)
- ใช้ข้อมูล PassMark เพื่ออ้างอิงเท่านั้น ไม่ได้นำไปเทรนโมเดล ตาม Content-Signal ใน robots.txt ของเว็บ

## สถาปัตยกรรม

```
start
 └─ extract_sources (TaskGroup, รันขนานกัน)
      ├─ extract_ihavecpu
      ├─ extract_passmark
      └─ extract_priceza[0..23]   ← Dynamic Task Mapping
 └─ merge_transform   trigger_rule=all_done → แหล่งไหนล่ม ก็รวมจากแหล่งที่เหลือต่อ
 └─ quality_gate      BranchPythonOperator: ≥100 แถวที่ผ่าน และสัดส่วนผ่าน ≥ 80%
      ├─ load_postgres      UPSERT (ON CONFLICT) → รันซ้ำแล้วจำนวนแถวไม่เพิ่ม
      └─ quarantine_batch   ไม่ผ่าน → ย้ายลง cpu_quarantine ไม่แตะข้อมูลหลัก
 └─ record_run        บันทึกลง pipeline_runs (ใช้ในหน้าสถานะ pipeline บนเว็บ)
```

**การ normalize และตัดรายการซ้ำข้ามแหล่ง**
- ชื่อสินค้าจากทุกแหล่งถูกแปลงเป็น `model_key` เดียวกันด้วย regex เช่น `CPU (ซีพียู) INTEL 1700 CORE I5-12400F 2.5GHz` และ `Intel Core i5-12400F` กลายเป็น `INTEL CORE I5-12400F`
- รุ่นนอกขอบเขต (โน้ตบุ๊ก, PRO, Threadripper, Intel รุ่นเก่า) ถูกตัดทิ้ง
- ตาราง `cpu_models` รวมเหลือ 1 แถวต่อ 1 รุ่น ประกอบด้วยราคาต่ำสุดในไทย, คะแนน PassMark, บาทต่อ 1,000 คะแนน และรายชื่อแหล่งที่พบ

**ตารางใน PostgreSQL (`cpu-db`)**

| ตาราง | หน้าที่ |
|---|---|
| `cpu_offers` | schema เดียวสำหรับทุกแหล่ง, PK = `(source, source_item_id)` |
| `cpu_models` | 1 แถวต่อรุ่น (ตัดซ้ำข้ามแหล่งแล้ว), PK = `model_key` |
| `cpu_quarantine` | แถวที่ไม่ผ่านกฎคุณภาพ พร้อมเหตุผล |
| `pipeline_runs` | สถานะแต่ละรอบ และสถานะแยกตามแหล่ง |

## วิธีรัน

ต้องมี Docker Desktop

```bash
cp .env.example .env          # แล้วแก้รหัสผ่านใน .env
docker compose up -d --build
```

| บริการ | URL |
|---|---|
| Airflow UI | http://localhost:8080 (ผู้ใช้/รหัสผ่านตาม `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` ใน `.env`) |
| Dashboard | http://localhost:8501 |
| PostgreSQL (ข้อมูล) | `localhost:5433` (ฐานข้อมูล/ผู้ใช้/รหัสผ่านตาม `.env`) |

- DAG `cpu_price_pipeline` เปิดใช้งานอัตโนมัติและรันรอบแรกเองหลังระบบขึ้น (ตาราง schedule ทุก 6 ชั่วโมง) ทั้งรอบใช้เวลาประมาณ 1–2 นาที
- จะสั่งรันเองก็ได้ด้วยปุ่ม ▶ ใน UI หรือคำสั่ง:
  ```bash
  docker compose exec airflow-scheduler airflow dags trigger cpu_price_pipeline
  ```

**ตรวจว่า UPSERT ไม่เพิ่มแถว:** trigger DAG ซ้ำ แล้วรันคำสั่งนี้ จำนวนแถวต้องเท่าเดิม
```bash
docker compose exec cpu-db sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT COUNT(*) FROM cpu_offers; SELECT COUNT(*) FROM cpu_models;"'
```

## โครงสร้าง repo

```
dags/cpu_price_pipeline.py   DAG
dags/cpu_sources.py          ฟังก์ชัน scrape และ normalize (ไม่พึ่ง Airflow)
web/app.py                   Streamlit dashboard + หน้าสถานะ pipeline
docker-compose.yml           Airflow (LocalExecutor) + PostgreSQL x2 + web
Dockerfile.airflow           apache/airflow:2.10.4 + requirements.txt
requirements.txt             ไลบรารีเพิ่มของ Airflow
.env.example                 ตัวอย่างตัวแปร (ห้าม commit .env)
```

## ผลการรัน

| รายการ | ผล |
|---|---|
| `cpu_offers` (รายการดิบจาก 3 แหล่ง) | 445 แถว (ihavecpu 41, passmark 212, priceza 192) |
| `cpu_models` (ตัดซ้ำข้ามแหล่งแล้ว) | 214 รุ่น |
| UPSERT | รัน DAG ซ้ำ (`manual_upsert_check`) จำนวนแถวคงเดิม 445 / 214 |
| แหล่งล่ม | `manual_passmark_down`: `extract_passmark` failed, `merge_transform` สำเร็จ, สถานะรัน success, `pipeline_runs.status = partial` |
| Quality gate ไม่ผ่าน | ตั้ง Variable `quality_min_rows=99999` แล้วรัน `manual_quality_fail`: `quarantine_batch` สำเร็จ, `load_postgres` ถูกข้าม, `pipeline_runs.status = quality_failed`, จำนวนแถวยังเป็น 445/214 |
| Quality gate (แถวผิดปกติ) | ตรวจพบราคาผิดปกติ (เช่น 9,999,999 บาท) และกักกันไว้ในตาราง quarantine |

### จำลองแหล่งข้อมูลล่ม

```bash
docker compose exec airflow-scheduler airflow variables set simulate_fail_source passmark   # ihavecpu | passmark | priceza
docker compose exec airflow-scheduler airflow dags trigger cpu_price_pipeline
# ยกเลิกการจำลอง
docker compose exec airflow-scheduler airflow variables delete simulate_fail_source
```

### จำลอง quality gate ไม่ผ่าน

```bash
docker compose exec airflow-scheduler airflow variables set quality_min_rows 99999
docker compose exec airflow-scheduler airflow dags trigger cpu_price_pipeline
docker compose exec airflow-scheduler airflow variables delete quality_min_rows   # ยกเลิก
```

หมายเหตุ: รหัสผ่านใน `.env` ใช้ได้เฉพาะตัวอักษรและตัวเลข เพราะถูกนำไปใส่ใน connection URI
