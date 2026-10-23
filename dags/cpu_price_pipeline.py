"""
cpu_price_pipeline — รวมราคา CPU (Intel Core 12–14, Core Ultra, AMD Ryzen เดสก์ท็อป) จาก 3 แหล่ง
เทียบกับคะแนน benchmark แล้วเก็บลง PostgreSQL

Graph:
  start
   └─ extract_sources (TaskGroup, ขนานกัน)
        ├─ extract_ihavecpu
        ├─ extract_passmark
        └─ extract_priceza  (Dynamic Task Mapping: 1 task ต่อ 1 คำค้น)
   └─ merge_transform       (trigger_rule=all_done -> แหล่งใดล่มก็รวมแหล่งที่เหลือต่อ)
   └─ quality_gate          (BranchPythonOperator)
        ├─ load_postgres    (UPSERT -> รันซ้ำจำนวนแถวไม่เพิ่ม)
        └─ quarantine_batch (คุณภาพไม่ผ่าน: เก็บลงตารางกักกัน ไม่แตะข้อมูลหลัก)
   └─ record_run            (บันทึกสถานะลง pipeline_runs ให้หน้าเว็บแสดง)
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

import cpu_sources as cs

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "cpu_db"
MIN_ROWS = 100            # ข้อกำหนดโจทย์: ข้อมูลรวม >= 100 รายการ
MIN_VALID_RATIO = 0.80    # สัดส่วนแถวที่ผ่านกฎคุณภาพขั้นต่ำ
MAX_PRICE_THB = 200_000

SOURCE_TASKS = {
    "ihavecpu": "extract_sources.extract_ihavecpu",
    "passmark": "extract_sources.extract_passmark",
    "priceza": "extract_sources.extract_priceza",
}

DDL = """
CREATE TABLE IF NOT EXISTS cpu_offers (
    source          TEXT NOT NULL,
    source_item_id  TEXT NOT NULL,
    model_key       TEXT NOT NULL,
    brand           TEXT NOT NULL,
    family          TEXT NOT NULL,
    generation      TEXT,
    product_name    TEXT,
    price_original  NUMERIC,
    currency        TEXT,
    price_min_thb   NUMERIC,
    price_max_thb   NUMERIC,
    offer_count     INTEGER,
    cpu_mark        INTEGER,
    url             TEXT,
    scraped_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (source, source_item_id)
);
CREATE TABLE IF NOT EXISTS cpu_models (
    model_key           TEXT PRIMARY KEY,
    brand               TEXT NOT NULL,
    family              TEXT NOT NULL,
    generation          TEXT,
    cpu_mark            INTEGER,
    best_price_thb      NUMERIC,
    avg_price_thb       NUMERIC,
    passmark_price_usd  NUMERIC,
    passmark_price_thb  NUMERIC,
    thb_per_1000_mark   NUMERIC,
    source_count        INTEGER,
    sources             TEXT,
    listing_count       INTEGER,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS cpu_quarantine (
    run_id      TEXT,
    source      TEXT,
    product_name TEXT,
    reason      TEXT,
    payload     JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    status          TEXT,
    quality_passed  BOOLEAN,
    total_rows      INTEGER,
    valid_rows      INTEGER,
    model_count     INTEGER,
    source_status   JSONB,
    usd_thb         NUMERIC,
    message         TEXT
);
"""


# ---------------------------------------------------------------- extract

def task_extract_ihavecpu(**_):
    return cs.extract_ihavecpu()


def task_extract_passmark(**_):
    # กรองขอบเขตตั้งแต่ต้นทาง ลดขนาด XCom (หน้า chart มี ~3,000 รุ่น)
    return [r for r in cs.extract_passmark() if cs.normalize_model(r["product_name"])]


def task_extract_priceza(query, **_):
    return cs.extract_priceza(query)


# ---------------------------------------------------------------- transform

def _pull_source(ti, name):
    """ดึงผลของแต่ละแหล่ง; แหล่งที่ fail จะได้ None -> นับเป็นล่ม แต่ไม่หยุด pipeline"""
    result = ti.xcom_pull(task_ids=SOURCE_TASKS[name])
    if name == "priceza":  # mapped task คืน list ของผลลัพธ์ทุก map index
        chunks = [chunk for chunk in (result or []) if chunk]
        return [row for chunk in chunks for row in chunk] if chunks else None
    return result


def validate_row(row):
    if not row.get("model_key"):
        return "นอกขอบเขต/แยกชื่อรุ่นไม่ได้"
    if row["source"] == "passmark":
        if not row.get("cpu_mark"):
            return "PassMark ไม่มีคะแนน CPU Mark"
        return None
    price = row.get("price_min_thb")
    if price is None:
        return "ไม่มีราคา"
    if not 500 <= price <= MAX_PRICE_THB:
        return f"ราคาผิดปกติ ({price})"
    return None


def task_merge_transform(ti, run_id, **_):
    usd_thb, rate_source = cs.fetch_usd_thb()
    source_status, rows = {}, []
    for name in SOURCE_TASKS:
        records = _pull_source(ti, name)
        if records is None:
            source_status[name] = {"status": "failed", "rows": 0}
            logger.error("แหล่ง %s ล่ม — รวมข้อมูลจากแหล่งที่เหลือต่อ", name)
            continue
        source_status[name] = {"status": "ok", "rows": len(records)}
        rows.extend(records)
    if not any(s["status"] == "ok" for s in source_status.values()):
        raise RuntimeError("ทุกแหล่งล่ม ไม่มีข้อมูลให้รวม")

    valid, rejected, seen = [], [], set()
    for row in rows:
        model = cs.normalize_model(row.get("product_name"))
        row.update(model or {"model_key": None})
        if row["source"] == "passmark" and row.get("price_original"):
            row["price_min_thb"] = row["price_max_thb"] = round(row["price_original"] * usd_thb, 2)
        key = (row["source"], row["source_item_id"])
        if key in seen:
            continue  # คำค้นต่างกันของ Priceza คืนสินค้าตัวเดิมได้ -> ตัดซ้ำในแหล่งเดียวกัน (ไม่ใช่ข้อมูลเสีย)
        seen.add(key)
        reason = validate_row(row)
        if reason:
            if row.get("model_key"):  # นอกขอบเขตไม่ต้องกักกัน แค่ข้าม
                rejected.append({"source": row["source"], "product_name": row.get("product_name"),
                                 "reason": reason, "payload": row})
            continue
        valid.append(row)

    in_scope = len(valid) + len(rejected)
    summary = {
        "run_id": run_id, "usd_thb": usd_thb, "rate_source": rate_source,
        "source_status": source_status, "total_rows": in_scope, "valid_rows": len(valid),
        "valid_ratio": round(len(valid) / in_scope, 3) if in_scope else 0,
        "model_count": len({r["model_key"] for r in valid}),
    }
    logger.info("merge summary: %s", summary)
    return {"summary": summary, "valid": valid, "rejected": rejected}


# ---------------------------------------------------------------- quality branch

def task_quality_gate(ti, **_):
    data = ti.xcom_pull(task_ids="merge_transform")
    s = data["summary"]
    passed = s["valid_rows"] >= MIN_ROWS and s["valid_ratio"] >= MIN_VALID_RATIO
    logger.info("quality: valid_rows=%s (>= %s) ratio=%s (>= %s) -> %s",
                s["valid_rows"], MIN_ROWS, s["valid_ratio"], MIN_VALID_RATIO, "PASS" if passed else "FAIL")
    return "load_postgres" if passed else "quarantine_batch"


def _hook():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(DDL)
    return hook


def _insert_quarantine(hook, run_id, rejected):
    if rejected:
        hook.insert_rows("cpu_quarantine", [
            (run_id, r["source"], r["product_name"], r["reason"], json.dumps(r["payload"], ensure_ascii=False, default=str))
            for r in rejected
        ], target_fields=["run_id", "source", "product_name", "reason", "payload"])


UPSERT_OFFER = """
INSERT INTO cpu_offers (source, source_item_id, model_key, brand, family, generation, product_name,
    price_original, currency, price_min_thb, price_max_thb, offer_count, cpu_mark, url, scraped_at, updated_at)
VALUES (%(source)s, %(source_item_id)s, %(model_key)s, %(brand)s, %(family)s, %(generation)s, %(product_name)s,
    %(price_original)s, %(currency)s, %(price_min_thb)s, %(price_max_thb)s, %(offer_count)s, %(cpu_mark)s,
    %(url)s, %(scraped_at)s, NOW())
ON CONFLICT (source, source_item_id) DO UPDATE SET
    model_key = EXCLUDED.model_key, brand = EXCLUDED.brand, family = EXCLUDED.family,
    generation = EXCLUDED.generation, product_name = EXCLUDED.product_name,
    price_original = EXCLUDED.price_original, currency = EXCLUDED.currency,
    price_min_thb = EXCLUDED.price_min_thb, price_max_thb = EXCLUDED.price_max_thb,
    offer_count = EXCLUDED.offer_count, cpu_mark = EXCLUDED.cpu_mark, url = EXCLUDED.url,
    scraped_at = EXCLUDED.scraped_at, updated_at = NOW();
"""

# ตัดรายการซ้ำข้ามแหล่ง: 1 แถวต่อ 1 รุ่น (model_key) รวมราคาจากร้านไทย + คะแนนจาก PassMark
UPSERT_MODELS = """
INSERT INTO cpu_models (model_key, brand, family, generation, cpu_mark, best_price_thb, avg_price_thb,
    passmark_price_usd, passmark_price_thb, thb_per_1000_mark, source_count, sources, listing_count, updated_at)
SELECT model_key,
       MIN(brand), MIN(family), MIN(generation),
       MAX(cpu_mark) FILTER (WHERE source = 'passmark'),
       MIN(price_min_thb) FILTER (WHERE source <> 'passmark'),
       ROUND(AVG(price_min_thb) FILTER (WHERE source <> 'passmark'), 2),
       MAX(price_original) FILTER (WHERE source = 'passmark'),
       MAX(price_min_thb) FILTER (WHERE source = 'passmark'),
       ROUND(COALESCE(MIN(price_min_thb) FILTER (WHERE source <> 'passmark'),
                      MAX(price_min_thb) FILTER (WHERE source = 'passmark'))
             / NULLIF(MAX(cpu_mark) FILTER (WHERE source = 'passmark'), 0) * 1000, 2),
       COUNT(DISTINCT source), STRING_AGG(DISTINCT source, ', '), COUNT(*), NOW()
FROM cpu_offers
GROUP BY model_key
ON CONFLICT (model_key) DO UPDATE SET
    brand = EXCLUDED.brand, family = EXCLUDED.family, generation = EXCLUDED.generation,
    cpu_mark = EXCLUDED.cpu_mark, best_price_thb = EXCLUDED.best_price_thb,
    avg_price_thb = EXCLUDED.avg_price_thb, passmark_price_usd = EXCLUDED.passmark_price_usd,
    passmark_price_thb = EXCLUDED.passmark_price_thb, thb_per_1000_mark = EXCLUDED.thb_per_1000_mark,
    source_count = EXCLUDED.source_count, sources = EXCLUDED.sources,
    listing_count = EXCLUDED.listing_count, updated_at = NOW();
"""


def task_load_postgres(ti, run_id, **_):
    data = ti.xcom_pull(task_ids="merge_transform")
    hook = _hook()
    conn = hook.get_conn()
    with conn, conn.cursor() as cur:
        for row in data["valid"]:
            cur.execute(UPSERT_OFFER, {k: row.get(k) for k in (
                "source", "source_item_id", "model_key", "brand", "family", "generation", "product_name",
                "price_original", "currency", "price_min_thb", "price_max_thb", "offer_count", "cpu_mark",
                "url", "scraped_at")})
        cur.execute(UPSERT_MODELS)
    _insert_quarantine(hook, run_id, data["rejected"])
    offers = hook.get_first("SELECT COUNT(*) FROM cpu_offers")[0]
    models = hook.get_first("SELECT COUNT(*) FROM cpu_models")[0]
    logger.info("UPSERT เสร็จ: cpu_offers=%s แถว, cpu_models=%s รุ่น (รันซ้ำแล้วจำนวนไม่เพิ่ม)", offers, models)


def task_quarantine_batch(ti, run_id, **_):
    data = ti.xcom_pull(task_ids="merge_transform")
    batch = data["rejected"] + [{"source": r["source"], "product_name": r["product_name"],
                                 "reason": "batch ไม่ผ่าน quality gate", "payload": r} for r in data["valid"]]
    _insert_quarantine(_hook(), run_id, batch)
    logger.warning("คุณภาพไม่ผ่าน: กักกัน %d แถว ไม่แตะตารางหลัก", len(batch))


def task_record_run(ti, run_id, dag_run, **_):
    data = ti.xcom_pull(task_ids="merge_transform") or {}
    s = data.get("summary", {})
    loaded = ti.xcom_pull(task_ids="quality_gate") == "load_postgres"
    failed = [k for k, v in s.get("source_status", {}).items() if v["status"] != "ok"]
    status = "success" if loaded and not failed else ("partial" if loaded else "quality_failed")
    message = f"แหล่งที่ล่ม: {', '.join(failed)}" if failed else "ทุกแหล่งปกติ"
    _hook().run("""
        INSERT INTO pipeline_runs (run_id, started_at, finished_at, status, quality_passed, total_rows,
            valid_rows, model_count, source_status, usd_thb, message)
        VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET finished_at = NOW(), status = EXCLUDED.status,
            quality_passed = EXCLUDED.quality_passed, total_rows = EXCLUDED.total_rows,
            valid_rows = EXCLUDED.valid_rows, model_count = EXCLUDED.model_count,
            source_status = EXCLUDED.source_status, usd_thb = EXCLUDED.usd_thb, message = EXCLUDED.message
    """, parameters=(run_id, dag_run.start_date or datetime.now(timezone.utc), status, loaded,
                     s.get("total_rows"), s.get("valid_rows"), s.get("model_count"),
                     json.dumps(s.get("source_status", {})), s.get("usd_thb"), message))


default_args = {"owner": "6621600917", "retries": 1, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="cpu_price_pipeline",
    description="ราคา CPU Intel 12–14 / Core Ultra / AMD Ryzen จาก 3 แหล่ง -> PostgreSQL",
    default_args=default_args,
    schedule="0 */6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["final", "cpu", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("extract_sources") as extract_sources:
        PythonOperator(task_id="extract_ihavecpu", python_callable=task_extract_ihavecpu)
        PythonOperator(task_id="extract_passmark", python_callable=task_extract_passmark)
        PythonOperator.partial(
            task_id="extract_priceza",
            python_callable=task_extract_priceza,
            max_active_tis_per_dag=2,  # จำกัดยิงพร้อมกันไม่เกิน 2 request ต่อเว็บ
        ).expand(op_kwargs=[{"query": q} for q in cs.PRICEZA_QUERIES])

    merge = PythonOperator(task_id="merge_transform", python_callable=task_merge_transform,
                           trigger_rule=TriggerRule.ALL_DONE)
    gate = BranchPythonOperator(task_id="quality_gate", python_callable=task_quality_gate)
    load = PythonOperator(task_id="load_postgres", python_callable=task_load_postgres)
    quarantine = PythonOperator(task_id="quarantine_batch", python_callable=task_quarantine_batch)
    record = PythonOperator(task_id="record_run", python_callable=task_record_run,
                            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    start >> extract_sources >> merge >> gate >> [load, quarantine] >> record
