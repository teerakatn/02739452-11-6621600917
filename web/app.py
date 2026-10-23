"""เทียบราคา CPU — dashboard อ่านข้อมูลจาก PostgreSQL โดยตรง (cpu_models, cpu_offers, pipeline_runs, cpu_quarantine)

หน้า "ราคา CPU": ผู้ซื้อเลือกงบ -> เห็น 3 รุ่นที่คุ้มที่สุด (ความคุ้ม = CPU Mark ต่อเงิน ฿1,000), กราฟ, ตาราง
หน้า "สถานะ Pipeline": สถานะรอบรันล่าสุดของ DAG
ดีไซน์ตามต้นแบบ design/dashboard-redesign.html (ธีม grape); สไตล์อยู่ใน style.css และ .streamlit/config.toml
"""

import html
import json
import os
import re
import warnings
from urllib.parse import urlencode
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")  # อ่านผ่าน psycopg2 โดยตรง
st.set_page_config(page_title="เทียบราคา CPU", page_icon="฿", layout="wide", initial_sidebar_state="collapsed")

DB = dict(
    host=os.getenv("DB_HOST", "cpu-db"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.environ["DATA_DB_NAME"],
    user=os.environ["DATA_DB_USER"],
    password=os.environ["DATA_DB_PASSWORD"],
)
CSS = (Path(__file__).with_name("style.css")).read_text(encoding="utf-8")


@st.cache_data(ttl=60)
def query(sql):
    with psycopg2.connect(**DB) as conn:
        return pd.read_sql(sql, conn)


def safe_query(sql, quiet=False):
    try:
        return query(sql)
    except Exception as exc:  # ตารางยังไม่ถูกสร้างก่อน DAG รันครั้งแรก
        if not quiet:
            st.warning(f"ยังอ่านข้อมูลไม่ได้ (DAG อาจยังไม่รันครั้งแรก): {exc}")
        return pd.DataFrame()


# ------------------------------------------------------------------ helpers
INTEL, AMD = "#3A6EA5", "#C2524A"
COLOR = {"Intel": INTEL, "AMD": AMD}
THAI_MONTH = ["ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]
SOURCE_LABEL = {"ihavecpu": "ihavecpu", "priceza": "Priceza", "passmark": "PassMark"}
PRESETS = [  # (ป้ายปุ่ม, ต่ำสุด, สูงสุด; None = ไม่จำกัด)
    ("ไม่เกิน 5,000", 0, 5000),
    ("5,000–10,000", 5000, 10000),
    ("10,000–20,000", 10000, 20000),
    ("20,000 ขึ้นไป", 20000, None),
]
PRESET_RANGE = {label: (lo, hi) for label, lo, hi in PRESETS}
BRANDS = ["ทั้งหมด", "Intel", "AMD"]
ARROW = ('<svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><path d="M5 3h6v6M11 3 3.5 10.5" fill="none" '
         'stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>')

esc = html.escape


def safe_url(u):
    """ลิงก์จากข้อมูลที่ scrape มา: รับเฉพาะ http(s) กัน javascript:/data: แล้ว escape สำหรับใส่ใน attribute"""
    u = clean(u)
    return esc(u) if isinstance(u, str) and re.match(r"^https?://", u.strip(), re.I) else None


def fmt(n):
    return f"{round(n):,}"


def baht(n):
    return "฿" + fmt(n)


def nice(key):
    """'INTEL CORE ULTRA 5 245KF' -> 'Core Ultra 5 245KF', 'AMD RYZEN 7 7800X3D' -> 'Ryzen 7 7800X3D'"""
    out = []
    for w in re.sub(r"^(INTEL|AMD)\s+", "", key).split(" "):
        if w in ("CORE", "ULTRA", "RYZEN"):
            w = w[0] + w[1:].lower()
        elif re.match(r"^I\d-", w):
            w = "i" + w[1:]
        out.append(w)
    return " ".join(out)


def thai_datetime(ts):
    t = pd.Timestamp(ts)
    t = t.tz_localize("UTC") if t.tzinfo is None else t
    t = t.tz_convert("Asia/Bangkok")
    return f"{t.day} {THAI_MONTH[t.month - 1]} {t.year + 543} {t:%H:%M} น."


def clean(value):
    """NaN/None/'' -> None (pandas เติม NaN ให้ค่าที่ไม่มี)"""
    return None if value is None or (isinstance(value, float) and pd.isna(value)) or value == "" else value


def as_dict(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def html_block(s):
    """st.markdown ตีความบรรทัดว่าง/ย่อหน้าใน HTML -> รวมเป็นบรรทัดเดียว"""
    return re.sub(r">\s+<", "><", s.replace("\n", " ")).strip()


# ------------------------------------------------------------------ shell (header + freshness strip)
def freshness_html():
    runs = safe_query("SELECT * FROM pipeline_runs WHERE status = 'success' ORDER BY finished_at DESC NULLS LAST LIMIT 1", quiet=True)
    if runs.empty:
        return '<div class="fresh"><div class="fresh-in"><span><span class="dot bad"></span>ยังไม่มีรอบรันที่สำเร็จ</span></div></div>'
    r = runs.iloc[0]
    src = as_dict(r["source_status"])
    parts = []
    for name, st_ in src.items():
        label = esc(SOURCE_LABEL.get(name, str(name)))
        st_ = st_ if isinstance(st_, dict) else {}
        parts.append(f"{label} <b>{fmt(st_.get('rows') or 0)}</b>" if st_.get("status") == "ok" else f"{label} <b>ล่ม</b>")
    sep = '<span class="sep">|</span>'
    return html_block(f"""<div class="fresh"><div class="fresh-in">
      <span><span class="dot"></span>อัปเดตล่าสุด <b>{thai_datetime(r['finished_at'])}</b></span>{sep}
      <span>ดึงจาก {' · '.join(parts)} รายการ</span>{sep}
      <span>ผ่านตรวจคุณภาพ <b>{fmt(r['valid_rows'] or 0)}/{fmt(r['total_rows'] or 0)}</b></span><span class="sep hide-sm">|</span>
      <span class="hide-sm">รวมเป็น <b>{fmt(r['model_count'] or 0)}</b> รุ่น · อัปเดตทุก 6 ชั่วโมง</span>
    </div></div>""")


def render_shell(active):
    def tab(page, label):  # ลิงก์แท็บพาตัวกรองปัจจุบันไปด้วย (หน้าโหลดใหม่ทั้งหน้า session_state จึงหาย)
        cur = ' aria-current="page"' if page == active else ""
        return f'<a href="?{esc(urlencode(state_params(page), doseq=True))}" target="_self"{cur}>{label}</a>'

    header = html_block(f"""<header class="shell"><div class="shell-in">
      <a class="brand" href="?{esc(urlencode(state_params('prices'), doseq=True))}" target="_self"><span class="brand-mark" aria-hidden="true">฿</span>
      <span class="brand-name">เทียบราคา CPU<span class="brand-sub">Intel Core 12–14 · Core Ultra · AMD Ryzen — ราคาร้านในไทย เทียบคะแนน PassMark</span></span></a>
      <nav class="tabs" aria-label="หน้า">{tab('prices', 'ราคา CPU')}{tab('status', 'สถานะ Pipeline')}</nav>
    </div></header>""")
    st.markdown(f"<style>{CSS}</style>" + header + freshness_html() + '<div class="page-top"></div>', unsafe_allow_html=True)


# ------------------------------------------------------------------ data
def load_models():
    """cpu_models + ข้อมูลจาก cpu_offers -> 1 แถวต่อ 1 รุ่นที่มี CPU Mark และราคา (ไทย หรือ ราคาประมาณจาก PassMark)"""
    models = safe_query("SELECT * FROM cpu_models ORDER BY brand, family, model_key")
    offers = safe_query("SELECT * FROM cpu_offers")
    if models.empty:
        return None, offers
    for col in ["cpu_mark", "best_price_thb", "passmark_price_thb"]:
        models[col] = pd.to_numeric(models[col], errors="coerce")
    for col in ["price_min_thb", "price_max_thb", "offer_count"]:
        if col in offers:
            offers[col] = pd.to_numeric(offers[col], errors="coerce")

    shops = url = purl = pd.Series(dtype="object")
    if not offers.empty:
        thai = offers[offers["source"] != "passmark"]
        n = thai["offer_count"].fillna(0)
        shops = thai.assign(n=n.where(n > 0, 1)).groupby("model_key")["n"].sum()  # ร้านต่อรุ่น = ผลรวม offer_count
        cheapest = thai.dropna(subset=["price_min_thb"]).sort_values("price_min_thb").drop_duplicates("model_key")
        url = cheapest.set_index("model_key")["url"]
        purl = offers[offers["source"] == "passmark"].drop_duplicates("model_key").set_index("model_key")["url"]

    m = models[models["cpu_mark"].notna() & (models["cpu_mark"] > 0)].copy()  # ไม่มี CPU Mark = ตัดออก
    m["priced"] = m["best_price_thb"].notna()
    m["price"] = m["best_price_thb"].where(m["priced"], m["passmark_price_thb"])
    m = m[m["price"].notna() & (m["price"] > 0)].copy()
    m["value"] = m["cpu_mark"] / m["price"] * 1000  # คะแนน CPU Mark ต่อ ฿1,000
    m["name"] = m["model_key"].map(nice)
    m["shops"] = m["model_key"].map(shops).fillna(0).astype(int)
    m["url"] = m["model_key"].map(url)
    m["purl"] = m["model_key"].map(purl)
    return m.reset_index(drop=True), offers


# ------------------------------------------------------------------ dashboard: state + controls
def parse_baht(text):
    """'฿5,000' / '5000' -> 5000, '' -> None"""
    digits = re.sub(r"\D", "", text or "")
    return int(digits) if digits else None


def set_budget(lo, hi):
    """ตั้งงบ (ตัวเลข) + ข้อความในช่องกรอก (มีคอมมา) + ปุ่มช่วงงบที่ตรงกัน"""
    s = st.session_state
    s.bmin, s.bmax = lo, hi
    s.bmin_s, s.bmax_s = fmt(lo), ("" if hi is None else fmt(hi))
    s.preset = next((lbl for lbl, a, b in PRESETS if (a, b) == (lo, hi)), None)


def init_state():
    """ค่าเริ่มต้นมาจาก query string (?lo=&hi=&brand=&fam=&est=1) เพื่อให้สลับแท็บ/รีโหลดแล้วตัวกรองไม่หาย"""
    s, qp = st.session_state, st.query_params
    if "bmin" not in s:
        if "lo" in qp or "hi" in qp:
            lo, hi = parse_baht(qp.get("lo")) or 0, parse_baht(qp.get("hi"))
            if hi is not None and lo > hi:
                lo, hi = hi, lo
        else:
            lo, hi = 5000, 10000
        set_budget(lo, hi)
    s.setdefault("brand", qp.get("brand") if qp.get("brand") in BRANDS[1:] else "ทั้งหมด")
    s.setdefault("fams", qp.get_all("fam"))
    s.setdefault("unpriced", qp.get("est") == "1")


def state_params(page):
    s = st.session_state
    params = {"page": page, "lo": str(s.bmin)}
    if s.bmax is not None:
        params["hi"] = str(s.bmax)
    if s.brand in BRANDS[1:]:
        params["brand"] = s.brand
    if s.fams:
        params["fam"] = list(s.fams)
    if s.unpriced:
        params["est"] = "1"
    return params


def on_preset():
    rng = PRESET_RANGE.get(st.session_state.preset)
    if rng:
        set_budget(*rng)


def on_budget_input():
    lo, hi = parse_baht(st.session_state.bmin_s) or 0, parse_baht(st.session_state.bmax_s)
    if hi is not None and lo > hi:
        lo, hi = hi, lo
    set_budget(lo, hi)


def widen_budget():
    set_budget(0, None)


def render_controls(all_models):
    with st.container(border=True, key="controls"):
        left, right = st.columns([5, 3])
        with left:
            st.markdown('<h1 class="q-title">มีงบเท่าไร?</h1>', unsafe_allow_html=True)
            st.radio("ช่วงงบ", [p[0] for p in PRESETS], index=None, horizontal=True, key="preset", on_change=on_preset,
                     label_visibility="collapsed")
        with right:
            with st.container(key="range"):
                c1, c2, c3 = st.columns([5, 1, 5])
                # text_input แทน number_input: แสดง "5,000" มีคอมมาได้ (number_input ของ Streamlit 1.39 แสดง 5000)
                with c1:
                    st.text_input("งบต่ำสุด (บาท)", key="bmin_s", on_change=on_budget_input, max_chars=9,
                                  label_visibility="collapsed")
                with c2:
                    st.markdown('<span class="to">ถึง</span>', unsafe_allow_html=True)
                with c3:
                    st.text_input("งบสูงสุด (บาท)", key="bmax_s", placeholder="ไม่จำกัด", on_change=on_budget_input,
                                  max_chars=9, label_visibility="collapsed")
        with st.container(key="filters"):
            f1, f2, f3 = st.columns([3, 4, 5])
            with f1:
                st.radio("แบรนด์", BRANDS, horizontal=True, key="brand")
            with f2:
                families = sorted(all_models[["brand", "family"]].drop_duplicates().itertuples(index=False))
                options = [f for _, f in families]
                st.session_state.fams = [f for f in st.session_state.fams if f in options]  # ค่าจาก URL ที่ไม่มีจริงทิ้งไป
                st.multiselect("ตระกูล", options, key="fams", placeholder="ทุกตระกูล")
            with f3:
                st.toggle("รวมรุ่นที่ยังไม่มีขายในไทย (ใช้ราคาประมาณจาก PassMark)", key="unpriced")


def budget_text(lo, hi):
    if hi is None:
        return f"{baht(lo)} ขึ้นไป"
    if not lo:
        return f"ไม่เกิน {baht(hi)}"
    return f"{baht(lo)}–{fmt(hi)}"


# ------------------------------------------------------------------ dashboard: sections
def section_head(title, sub="", sub_class=""):
    st.markdown(html_block(f'<div class="sec-h"><h2>{esc(title)}</h2><p class="{sub_class}">{esc(sub)}</p></div>'), unsafe_allow_html=True)


def shop_link(row, cls, label):
    url, purl = safe_url(row["url"]), safe_url(row["purl"])
    if url:
        return f'<a class="{cls}" href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>'
    if purl:
        return f'<a class="{cls}" href="{purl}" target="_blank" rel="noopener noreferrer">ดูที่ PassMark</a>'
    return f'<span class="{cls} off">ไม่มีลิงก์</span>'


def render_picks(in_budget, scope, lo, hi):
    section_head("คุ้มที่สุดในทุกช่วงราคา" if lo == 0 and hi is None else f"คุ้มที่สุดในงบ {budget_text(lo, hi)}",
                 f"จาก {len(in_budget)} รุ่นในงบนี้ · ความคุ้ม = คะแนน CPU Mark ต่อเงิน ฿1,000 ยิ่งมากยิ่งดี" if len(in_budget) else "")
    if in_budget.empty:
        with st.container(key="empty"):
            if scope.empty:
                hint = "ลองเปลี่ยนแบรนด์หรือตระกูลที่เลือกไว้"
            else:
                dist = scope["price"].map(lambda p: max(lo - p, 0 if hi is None else p - hi, 0))
                near = scope.loc[dist.idxmin()]
                hint = f"รุ่นที่ราคาใกล้งบที่สุดคือ {near['name']} ราคา {baht(near['price'])}"
            st.markdown(f"<h3>ไม่มีรุ่นในงบนี้</h3><p>{esc(hint)}</p>", unsafe_allow_html=True)
            st.button("ดูทุกช่วงราคา", on_click=widen_budget)
        return
    max_v = in_budget["value"].max()
    cards = []
    for i, (_, m) in enumerate(in_budget.head(3).iterrows()):
        rank = '<i>1</i>คุ้มที่สุด' if i == 0 else f"อันดับ {i + 1}"
        price_note = "<small>ราคาต่ำสุดที่พบ</small>" if m["priced"] else '<span class="est">ราคาประมาณ</span>'
        shops = f"พบ <b>{m['shops']}</b> ร้าน" if m["shops"] else "<span>ยังไม่พบในร้านไทย</span>"
        cards.append(f"""<article class="pick{' first' if i == 0 else ''}"><span class="rank">{rank}</span>
          <div class="fam-line"><span class="bd {esc(m['brand'])}"></span>{esc(m['brand'])} · {esc(clean(m['generation']) or m['family'])}</div>
          <div class="model">{esc(m['name'])}</div>
          <div class="price">{baht(m['price'])}{price_note}</div>
          <div class="val"><div class="val-top"><span>ความคุ้ม</span><span><b>{fmt(m['value'])}</b> คะแนน / ฿1,000</span></div>
          <div class="bar"><span style="width:{m['value'] / max_v * 100:.1f}%"></span></div></div>
          <ul class="facts"><li>CPU Mark <b>{fmt(m['cpu_mark'])}</b></li><li>{shops}</li></ul>
          {shop_link(m, 'go', f'ดูราคาที่ร้าน {ARROW}')}</article>""")
    st.markdown(html_block(f'<div class="picks">{"".join(cards)}</div>'), unsafe_allow_html=True)


def build_chart(scope, picks, lo, hi):
    x_max, y_max = scope["price"].max() * 1.05, scope["cpu_mark"].max() * 1.08
    traces = []
    for brand in ("Intel", "AMD"):
        pts = scope[scope["brand"] == brand]
        in_b = [(p >= lo) and (hi is None or p <= hi) for p in pts["price"]]
        traces.append(go.Scatter(
            mode="markers", name=brand, x=pts["price"], y=pts["cpu_mark"], text=pts["name"],
            customdata=[[fmt(v), "" if pr else " (ราคาประมาณ)"] for v, pr in zip(pts["value"], pts["priced"])],
            hovertemplate="<b>%{text}</b><br>ราคา ฿%{x:,.0f}%{customdata[1]}<br>CPU Mark %{y:,}<br>"
                          "ความคุ้ม %{customdata[0]} คะแนน/฿1,000<extra></extra>",
            marker=dict(size=9, color=COLOR[brand], opacity=[0.95 if b else 0.28 for b in in_b],
                        symbol=["circle" if pr else "circle-open" for pr in pts["priced"]],
                        line=dict(width=[1 if pr else 1.6 for pr in pts["priced"]],
                                  color=["#fff" if pr else COLOR[brand] for pr in pts["priced"]]))))
    shapes = [dict(type="rect", xref="x", yref="paper", x0=lo, x1=x_max if hi is None else hi, y0=0, y1=1,
                   fillcolor="rgba(76,201,160,.14)", line=dict(color="rgba(47,174,133,.5)", width=1), layer="below")]
    ann = []
    for v in (2000, 4000, 6000):  # เส้นความคุ้มเท่ากัน (คะแนน ต่อ ฿1,000)
        x_end = min(x_max, y_max / v * 1000)
        shapes.append(dict(type="line", x0=0, y0=0, x1=x_end, y1=x_end * v / 1000, line=dict(color="#A9B5B0", width=1.2, dash="dash"), layer="below"))
        lx = x_end * 0.93
        ann.append(dict(x=lx, y=lx * v / 1000, text=fmt(v), showarrow=False, xanchor="left", yanchor="top", xshift=6,
                        font=dict(size=11.5, color="#6E7A75")))
    ann.append(dict(xref="paper", yref="paper", x=0.012, y=0.985, text="↖ คุ้มกว่า", showarrow=False, xanchor="left", yanchor="top",
                    font=dict(size=13.5, color="#1F7D62", family="Anuphan")))
    # ป้ายอันดับ 1–3 กระจายแนวตั้งตามคะแนน ไม่ให้เส้นนำทับกัน
    order = sorted(range(len(picks)), key=lambda i: -picks.iloc[i]["cpu_mark"])
    slot = {idx: [-46, -8, 30][j] for j, idx in enumerate(order)}
    for i, (_, m) in enumerate(picks.iterrows()):
        first = i == 0
        ann.append(dict(x=m["price"], y=m["cpu_mark"], text=f"<b>{i + 1}</b>  {m['name']}", showarrow=True, arrowhead=0, arrowwidth=1,
                        arrowcolor="#2FAE85", ax=64, ay=slot[i], xanchor="left", borderpad=4,
                        bgcolor="#4CC9A0" if first else "#DCF5EC", bordercolor="#2FAE85" if first else "#9FDEC8",
                        font=dict(size=12.5, color="#0B3326" if first else "#0F5A43", family="Anuphan")))
    fig = go.Figure(traces)
    fig.update_layout(
        height=440, margin=dict(l=64, r=18, t=10, b=52), paper_bgcolor="#fff", plot_bgcolor="#fff", showlegend=False, hovermode="closest",
        font=dict(family="Anuphan, system-ui, sans-serif", size=12.5, color="#4E5B56"),
        xaxis=dict(title=dict(text="ราคาต่ำสุดในไทย (บาท)", standoff=12), range=[0, x_max], tickprefix="฿", tickformat=",.0f",
                   gridcolor="#EEF2F0", zeroline=False, linecolor="#C6D0CC"),
        yaxis=dict(title=dict(text="คะแนน PassMark CPU Mark", standoff=10), range=[0, y_max], tickformat=",.0f",
                   gridcolor="#EEF2F0", zeroline=False, linecolor="#C6D0CC"),
        shapes=shapes, annotations=ann,
        hoverlabel=dict(bgcolor="#17201D", bordercolor="#17201D", font=dict(family="Anuphan", color="#fff", size=13)))
    return fig


def render_chart(scope, picks, lo, hi):
    section_head("ราคาเทียบประสิทธิภาพ", "จุดยิ่งอยู่ซ้ายบน ยิ่งได้คะแนนมากต่อเงินที่จ่าย")
    with st.container(border=True, key="chart"):
        if scope.empty:
            st.info("ไม่มีรุ่นที่ตรงกับตัวกรอง")
            return
        st.markdown(html_block("""<div class="legend"><span><i class="bd Intel"></i>Intel</span><span><i class="bd AMD"></i>AMD</span>
          <span><i class="band"></i>ช่วงงบที่เลือก</span><span><i class="iso"></i>เส้นความคุ้มเท่ากัน (คะแนน / ฿1,000)</span>
          <span><svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true"><circle cx="7" cy="7" r="4.5" fill="none" stroke="#626E69" stroke-width="1.6"/></svg>วงกลมโปร่ง = ราคาประมาณ</span></div>"""),
                    unsafe_allow_html=True)
        st.plotly_chart(build_chart(scope, picks, lo, hi), use_container_width=True, theme=None, config={"displayModeBar": False})


def render_table(in_budget):
    section_head(f"ทุกรุ่นในงบนี้ ({len(in_budget)})", "เรียงจากคุ้มที่สุด คลิกหัวคอลัมน์เพื่อเรียงใหม่")
    if in_budget.empty:
        st.caption("ไม่มีรุ่นที่ตรงกับตัวกรอง")
        return
    df = pd.DataFrame({
        "รุ่น": [n + ("" if p else " (ราคาประมาณ)") for n, p in zip(in_budget["name"], in_budget["priced"])],
        "ตระกูล": in_budget["brand"] + " · " + in_budget["family"],
        "ราคาต่ำสุด": in_budget["price"].round(),
        "CPU Mark": in_budget["cpu_mark"].astype(int),
        "ความคุ้ม": in_budget["value"].round(),
        "ร้าน": in_budget["shops"].where(in_budget["shops"] > 0).astype("Int64"),
        "ดูราคา": [u if safe_url(u) else (p if safe_url(p) else None) for u, p in zip(in_budget["url"], in_budget["purl"])],
    })
    # Streamlit 1.39 NumberColumn ใช้ printf (ไม่มีคอมมา) -> จัดรูปแบบด้วย pandas Styler แทน
    # ค่าที่แสดงมาจาก Styler แต่ข้อมูลจริงยังเป็นตัวเลข จึงคลิกหัวคอลัมน์เรียงตามตัวเลขได้ถูกต้อง
    # (ProgressColumn ไม่ใช้ค่าจาก Styler จึงยังแสดง 6284 ไม่มีคอมมา)
    styled = df.style.format({"ราคาต่ำสุด": "฿{:,.0f}", "CPU Mark": "{:,.0f}"}).format(
        "{:,.0f}", subset=["ร้าน"], na_rep="–")
    st.dataframe(
        styled, hide_index=True, use_container_width=True, height=min(len(df), 15) * 35 + 39,
        column_config={
            "รุ่น": st.column_config.TextColumn("รุ่น", width="medium"),
            "ตระกูล": st.column_config.TextColumn("ตระกูล", width="small"),
            "ราคาต่ำสุด": st.column_config.NumberColumn("ราคาต่ำสุด"),
            "CPU Mark": st.column_config.NumberColumn("CPU Mark"),
            "ความคุ้ม": st.column_config.ProgressColumn("ความคุ้ม (คะแนน/฿1,000)", format="%d", min_value=0,
                                                       max_value=float(df["ความคุ้ม"].max()), width="medium"),
            "ร้าน": st.column_config.NumberColumn("ร้าน", width="small"),
            # แสดงชื่อเว็บจากโดเมนของลิงก์ (ihavecpu / priceza / cpubenchmark)
            "ดูราคา": st.column_config.LinkColumn("ดูราคา", display_text=r"https?://(?:www\.)?([^./]+)\."),
        })


def render_raw(offers):
    if offers.empty:
        return
    with st.expander(f"ข้อมูลดิบจากแต่ละแหล่ง · cpu_offers {len(offers):,} รายการ ก่อนตัดซ้ำข้ามแหล่ง"):
        raw = offers[["source", "model_key", "product_name", "price_min_thb", "price_max_thb", "offer_count", "url"]]
        st.dataframe(raw, hide_index=True, use_container_width=True, column_config={
            "source": "แหล่ง", "model_key": "รุ่น (model_key)", "product_name": "ชื่อสินค้าตามแหล่ง",
            "price_min_thb": st.column_config.NumberColumn("ราคาต่ำสุด", format="฿%d"),
            "price_max_thb": st.column_config.NumberColumn("สูงสุด", format="฿%d"),
            "offer_count": st.column_config.NumberColumn("ร้าน", format="%d"),
            "url": st.column_config.LinkColumn("ลิงก์", display_text="เปิด"),
        })


def dashboard_page():
    models, offers = load_models()
    if models is None or models.empty:
        return
    render_controls(models)
    st.query_params.from_dict(state_params("prices"))  # URL สะท้อนตัวกรองปัจจุบัน (แชร์ลิงก์/รีโหลดได้)
    s = st.session_state
    lo, hi = int(s.bmin or 0), (None if s.bmax is None else int(s.bmax))
    scope = models if s.unpriced else models[models["priced"]]
    if s.brand != "ทั้งหมด":
        scope = scope[scope["brand"] == s.brand]
    if s.fams:
        scope = scope[scope["family"].isin(s.fams)]
    in_b = scope[(scope["price"] >= lo) & ((scope["price"] <= hi) if hi is not None else True)].sort_values("value", ascending=False)
    render_picks(in_b, scope, lo, hi)
    render_chart(scope, in_b.head(3), lo, hi)
    render_table(in_b)
    render_raw(offers)


# ------------------------------------------------------------------ pipeline status (โบนัส)
def status_page():
    st.markdown('<h1 class="q-title">สถานะ Pipeline <span style="font-weight:500;font-size:16px;color:var(--ink-3)">cpu_price_pipeline</span></h1>',
                unsafe_allow_html=True)
    runs = safe_query("SELECT * FROM pipeline_runs ORDER BY finished_at DESC NULLS LAST LIMIT 20")
    if runs.empty:
        st.info("ยังไม่มีประวัติการรัน")
        return
    last = runs.iloc[0]
    icon = {"success": "🟢", "partial": "🟡", "quality_failed": "🔴"}.get(last["status"], "⚪")
    a, b, c, d = st.columns(4)
    a.metric("สถานะล่าสุด", f"{icon} {last['status']}")
    b.metric("แถวที่ผ่านคุณภาพ", f"{int(last['valid_rows'] or 0):,} / {int(last['total_rows'] or 0):,}")
    c.metric("จำนวนรุ่น", f"{int(last['model_count'] or 0):,}")
    d.metric("USD→THB", f"{float(last['usd_thb'] or 0):.2f}")
    st.caption(f"รันล่าสุด: {last['finished_at']} · {last['message']}")

    st.subheader("สถานะแต่ละแหล่งในรอบล่าสุด")
    status = as_dict(last["source_status"])
    st.dataframe(pd.DataFrame([{"แหล่ง": k, "สถานะ": ("✅ " if v["status"] == "ok" else "❌ ") + v["status"], "จำนวนแถว": v["rows"]}
                               for k, v in status.items()]), hide_index=True)

    st.subheader("ประวัติการรัน")
    st.dataframe(runs[["run_id", "status", "quality_passed", "valid_rows", "total_rows", "model_count", "message", "finished_at"]],
                 use_container_width=True, hide_index=True)

    st.subheader("ตารางกักกัน (ไม่ผ่านกฎคุณภาพ) ล่าสุด")
    st.dataframe(safe_query("SELECT source, product_name, reason, created_at FROM cpu_quarantine ORDER BY created_at DESC LIMIT 50"),
                 use_container_width=True, hide_index=True)


# ------------------------------------------------------------------ main
page = st.query_params.get("page", "prices")
init_state()
render_shell("status" if page == "status" else "prices")
if page == "status":
    status_page()
else:
    dashboard_page()
