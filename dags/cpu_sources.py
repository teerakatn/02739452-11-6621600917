"""
cpu_sources.py — ฟังก์ชันดึงและแปลงข้อมูลราคา CPU (ไม่พึ่ง Airflow จึงทดสอบแยกได้)

ขอบเขต: Intel Core รุ่นที่ 12–14 (เดสก์ท็อป), Intel Core Ultra (เดสก์ท็อป ซีรีส์ 2xx),
        AMD Ryzen เดสก์ท็อป
แหล่ง : 1) ihavecpu.com      — scrape __NEXT_DATA__ ในหน้า HTML (ราคาบาท)
        2) priceza.com       — scrape การ์ดสินค้าในหน้าค้นหา /s/ (ช่วงราคาบาทจากหลายร้าน)
        3) cpubenchmark.net  — scrape หน้า chart ของ PassMark (คะแนน CPU Mark + ราคา USD)
จริยธรรม: ตั้ง User-Agent ระบุตัวตน, time.sleep ระหว่าง request, ตรวจ robots.txt ก่อนดึง,
         ไม่เก็บข้อมูลส่วนบุคคล (เช่น ชื่อร้าน/ผู้ขายใน Priceza) ตาม PDPA
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (compatible; KU-BigData-Exam-ETL/1.0; academic use; 02739452)"
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "th-TH,th;q=0.9,en;q=0.8"}
REQUEST_DELAY_SEC = 1.5
TIMEOUT_SEC = 30

IHAVECPU_URL = "https://ihavecpu.com/category/cpu"
PRICEZA_SEARCH_URL = "https://www.priceza.com/s/ราคา/{query}"
PASSMARK_PAGES = [
    "https://www.cpubenchmark.net/high_end_cpus.html",
    "https://www.cpubenchmark.net/mid_range_cpus.html",
]
# คำค้น Priceza หนึ่งคำต่อหนึ่ง mapped task (Dynamic Task Mapping)
PRICEZA_QUERIES = (
    [f"Intel Core i{tier}-{gen}" for gen in (12, 13, 14) for tier in (3, 5, 7, 9)]
    + [f"Intel Core Ultra {tier} 2" for tier in (5, 7, 9)]
    + [f"AMD Ryzen {tier} {series}" for series in (5, 7, 9) for tier in (5, 7, 9)]
)

# ---------------------------------------------------------------- HTTP

_robots = {}


def allowed_by_robots(url):
    origin = "{0.scheme}://{0.netloc}".format(urlparse(url))
    if origin not in _robots:
        parser = RobotFileParser(origin + "/robots.txt")
        try:
            response = requests.get(origin + "/robots.txt", headers=HEADERS, timeout=TIMEOUT_SEC)
            parser.parse(response.text.splitlines() if response.ok else [])
        except requests.RequestException as exc:
            logger.warning("อ่าน robots.txt ของ %s ไม่ได้: %s", origin, exc)
        _robots[origin] = parser
    return _robots[origin].can_fetch(USER_AGENT, url)


def polite_get(url, params=None):
    """GET ที่เคารพ robots.txt + หน่วงเวลา; 4xx ไม่ลองซ้ำ"""
    if not allowed_by_robots(url):
        raise PermissionError(f"robots.txt ไม่อนุญาต: {url}")
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT_SEC)
    finally:
        time.sleep(REQUEST_DELAY_SEC)
    response.raise_for_status()
    response.encoding = "utf-8"
    return response.text


# ---------------------------------------------------------------- normalize

INTEL_RE = re.compile(r"\bi([3579])[\s-]*(1[234]\d{3})([a-z]{0,2})\b", re.I)
ULTRA_RE = re.compile(r"\bultra\s*([3579])\s*(\d{3})([a-z]{0,2})\b", re.I)
RYZEN_RE = re.compile(r"\bryzen\s*([3579])\s*(pro\s*)?(\d{4})\s*(x3d|xt|x|gt|ge|g|f)?\b", re.I)

INTEL_DESKTOP_SUFFIX = {"", "K", "KF", "F", "KS", "T"}
ULTRA_DESKTOP_SUFFIX = {"", "K", "KF", "F", "T"}


def normalize_model(name):
    """แปลงชื่อสินค้าจากทุกแหล่งให้เป็น model_key เดียวกัน; นอกขอบเขตคืน None

    ตัวอย่าง: 'CPU (ซีพียู) INTEL 1700 CORE I5-12400F 2.5GHz' -> ('INTEL CORE I5-12400F', ...)
              'AMD Ryzen 7 7800X3D'                       -> ('AMD RYZEN 7 7800X3D', ...)
    """
    if not name:
        return None
    text = str(name)
    lowered = text.lower()
    if "threadripper" in lowered or "xeon" in lowered or "epyc" in lowered:
        return None

    match = ULTRA_RE.search(text)
    if match and "intel" in lowered or (match and "core" in lowered):
        tier, number, suffix = match.group(1), match.group(2), match.group(3).upper()
        if number.startswith("2") and suffix in ULTRA_DESKTOP_SUFFIX:  # 2xx = desktop Arrow Lake
            return {"model_key": f"INTEL CORE ULTRA {tier} {number}{suffix}", "brand": "Intel",
                    "family": f"Core Ultra {tier}", "generation": "Core Ultra (Series 2)"}
        return None

    match = INTEL_RE.search(text)
    if match and ("intel" in lowered or "core" in lowered):
        tier, number, suffix = match.group(1), match.group(2), match.group(3).upper()
        if suffix in INTEL_DESKTOP_SUFFIX:
            return {"model_key": f"INTEL CORE I{tier}-{number}{suffix}", "brand": "Intel",
                    "family": f"Core i{tier}", "generation": f"Gen {number[:2]}"}
        return None

    match = RYZEN_RE.search(text)
    if match:
        tier, is_pro, number, suffix = match.group(1), match.group(2), match.group(3), (match.group(4) or "").upper()
        tail = text[match.end():match.end() + 4].strip().upper()
        if is_pro or tail[:1] in {"U", "H"} or tail.startswith("HS") or tail.startswith("HX"):
            return None  # PRO / mobile ไม่อยู่ในขอบเขตเดสก์ท็อป
        return {"model_key": f"AMD RYZEN {tier} {number}{suffix}", "brand": "AMD",
                "family": f"Ryzen {tier}", "generation": f"Ryzen {number[0]}000"}
    return None


def to_number(text):
    if text is None:
        return None
    digits = re.sub(r"[^0-9.]", "", str(text).replace(",", ""))
    try:
        value = float(digits)
    except ValueError:
        return None
    return value if value > 0 else None


def _now():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------- source 1: ihavecpu

def extract_ihavecpu(max_pages=10):
    """ihavecpu เป็น Next.js — ข้อมูลสินค้าฝังใน <script id="__NEXT_DATA__"> ของหน้า HTML"""
    records, seen = [], set()
    for page in range(1, max_pages + 1):
        html = polite_get(IHAVECPU_URL, params={"page": page})
        tag = BeautifulSoup(html, "html.parser").find("script", id="__NEXT_DATA__")
        if tag is None:
            raise ValueError("ไม่พบ __NEXT_DATA__ — โครงสร้างเว็บ ihavecpu เปลี่ยน")
        items = json.loads(tag.string)["props"]["pageProps"]["product"]["data"]
        new_items = [item for item in items if item["product_id"] not in seen]
        if not new_items:
            break  # หน้าเกินจำนวนจริงจะคืนรายการซ้ำ/ว่าง
        for item in new_items:
            seen.add(item["product_id"])
            records.append({
                "source": "ihavecpu",
                "source_item_id": str(item["product_id"]),
                "product_name": item.get("name_th", ""),
                "price_original": to_number(item.get("price_sale")),
                "currency": "THB",
                "price_min_thb": to_number(item.get("price_sale")),
                "price_max_thb": to_number(item.get("price_sale")),
                "offer_count": 1,
                "cpu_mark": None,
                "in_stock": (item.get("stock") or 0) > 0 if isinstance(item.get("stock"), (int, float)) else None,
                "url": f"https://ihavecpu.com/product/{item['product_id']}",
                "scraped_at": _now(),
            })
    logger.info("ihavecpu: %d รายการ", len(records))
    return records


# ---------------------------------------------------------------- source 2: priceza

def extract_priceza(query):
    """หนึ่งคำค้นต่อหนึ่งครั้ง; เก็บเฉพาะชื่อรุ่น/ช่วงราคา/จำนวนร้าน ไม่เก็บชื่อผู้ขาย (PDPA)"""
    html = polite_get(PRICEZA_SEARCH_URL.format(query=quote(query)))
    cards = BeautifulSoup(html, "html.parser").select(".pz-pdb-item")
    records = []
    for card in cards:
        try:
            box = card.select_one("[data-productid]")
            name_tag = card.select_one(".pz-pdb_name a")
            price_box = card.select_one(".pz-pdb-price")
            if not (box and name_tag and price_box):
                continue
            # ช่วงราคาอยู่ใน attribute content="450.0 - 2208.0"; บางการ์ดมีแค่ข้อความ ฿5,320
            # (ต้องข้าม content="THB" ของสัญลักษณ์สกุลเงิน)
            numeric = [t["content"] for t in price_box.select("[content]") if re.search(r"\d", t["content"])]
            price_text = numeric[0] if numeric else price_box.get_text(" ", strip=True).replace("฿", " ")
            low, _, high = price_text.partition("-")
            offers = re.search(r"\((\d+)\)", card.select_one(".pz-btn-product-group").get_text()) \
                if card.select_one(".pz-btn-product-group") else None
            records.append({
                "source": "priceza",
                "source_item_id": box["data-productid"],
                "product_name": name_tag.get_text(" ", strip=True),
                "price_original": to_number(low),
                "currency": "THB",
                "price_min_thb": to_number(low),
                "price_max_thb": to_number(high) or to_number(low),
                "offer_count": int(offers.group(1)) if offers else 1,
                "cpu_mark": None,
                "in_stock": None,
                "url": "https://www.priceza.com" + name_tag.get("href", ""),
                "scraped_at": _now(),
            })
        except Exception as exc:  # การ์ดเดียวพังไม่ทำให้ทั้งหน้าพัง
            logger.warning("ข้ามการ์ด Priceza: %s", exc)
    logger.info("priceza '%s': %d การ์ด", query, len(records))
    return records


# ---------------------------------------------------------------- source 3: PassMark

def extract_passmark():
    """cpu_list.php ส่งมาเฉพาะ Intel จึงใช้หน้า chart high/mid-end ที่มีทั้ง Intel และ AMD"""
    records, seen = [], set()
    for page_url in PASSMARK_PAGES:
        soup = BeautifulSoup(polite_get(page_url), "html.parser")
        rows = soup.select("ul.chartlist li")
        if not rows:
            raise ValueError(f"ไม่พบ ul.chartlist ใน {page_url} — โครงสร้างเว็บเปลี่ยน")
        for row in rows:
            name = row.select_one(".prdname")
            if name is None:
                continue
            name = name.get_text(strip=True)
            if name in seen:
                continue
            seen.add(name)
            mark = row.select_one(".count")
            price = row.select_one(".price-neww")
            price_text = price.get_text(strip=True) if price else ""
            link = row.select_one("a[href]")
            records.append({
                "source": "passmark",
                "source_item_id": name,
                "product_name": name,
                "price_original": to_number(price_text) if "$" in price_text else None,
                "currency": "USD",
                "price_min_thb": None,  # แปลงด้วยอัตราแลกเปลี่ยนตอน transform
                "price_max_thb": None,
                "offer_count": 1,
                "cpu_mark": int(to_number(mark.get_text(strip=True))) if mark and to_number(mark.get_text(strip=True)) else None,
                "in_stock": None,
                "url": "https://www.cpubenchmark.net/" + link["href"].lstrip("/") if link else page_url,
                "scraped_at": _now(),
            })
    logger.info("passmark: %d รายการ (ก่อนกรองขอบเขต)", len(records))
    return records


def fetch_usd_thb(default=33.0):
    """อัตรา USD->THB สำหรับแปลงราคา PassMark; ถ้า API ล่มใช้ค่า default และ log ไว้"""
    try:
        response = requests.get("https://open.er-api.com/v6/latest/USD", headers=HEADERS, timeout=TIMEOUT_SEC)
        response.raise_for_status()
        rate = response.json()["rates"]["THB"]
        return float(rate), "open.er-api.com"
    except Exception as exc:
        logger.warning("ดึงอัตราแลกเปลี่ยนไม่ได้ ใช้ค่า default %.2f: %s", default, exc)
        return default, "default"
