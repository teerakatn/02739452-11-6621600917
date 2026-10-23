"""CPU Price Dashboard — อ่านข้อมูลจาก PostgreSQL โดยตรง (ตาราง cpu_models, cpu_offers, pipeline_runs)"""

import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="CPU Price Dashboard", page_icon="🖥️", layout="wide")

DB = dict(
    host=os.getenv("DB_HOST", "cpu-db"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.environ["DATA_DB_NAME"],
    user=os.environ["DATA_DB_USER"],
    password=os.environ["DATA_DB_PASSWORD"],
)


@st.cache_data(ttl=60)
def query(sql):
    with psycopg2.connect(**DB) as conn:
        return pd.read_sql(sql, conn)


def safe_query(sql):
    try:
        return query(sql)
    except Exception as exc:  # ตารางยังไม่ถูกสร้างก่อน DAG รันครั้งแรก
        st.warning(f"ยังอ่านข้อมูลไม่ได้ (DAG อาจยังไม่รันครั้งแรก): {exc}")
        return pd.DataFrame()


page = st.sidebar.radio("หน้า", ["📊 Dashboard ราคา CPU", "🛠️ สถานะ Pipeline"])

# ------------------------------------------------------------------ dashboard
if page.startswith("📊"):
    st.title("ราคา CPU Intel Core 12–14 / Core Ultra / AMD Ryzen ในไทย เทียบคะแนน benchmark")
    models = safe_query("SELECT * FROM cpu_models ORDER BY brand, family, model_key")
    offers = safe_query("SELECT * FROM cpu_offers")
    if models.empty:
        st.stop()
    for col in ["cpu_mark", "best_price_thb", "avg_price_thb", "passmark_price_usd", "passmark_price_thb", "thb_per_1000_mark"]:
        models[col] = pd.to_numeric(models[col], errors="coerce")

    st.sidebar.header("ตัวกรอง")
    brands = st.sidebar.multiselect("แบรนด์", sorted(models["brand"].unique()), default=sorted(models["brand"].unique()))
    families = st.sidebar.multiselect("ตระกูล", sorted(models["family"].unique()))
    sources = st.sidebar.multiselect("ต้องมีข้อมูลจากแหล่ง", ["ihavecpu", "priceza", "passmark"])
    priced_only = st.sidebar.checkbox("เฉพาะรุ่นที่มีราคาในไทย", value=True)
    price_max = float(models["best_price_thb"].max() or 1)
    price_range = st.sidebar.slider("ช่วงราคาในไทย (บาท)", 0.0, price_max, (0.0, price_max), step=100.0)

    view = models[models["brand"].isin(brands)]
    if families:
        view = view[view["family"].isin(families)]
    for source in sources:
        view = view[view["sources"].fillna("").str.contains(source)]
    if priced_only:
        view = view[view["best_price_thb"].notna()]
    view = view[view["best_price_thb"].isna() | view["best_price_thb"].between(*price_range)]

    # KPI
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("จำนวนรุ่น (ตัดซ้ำข้ามแหล่งแล้ว)", f"{len(view):,}")
    k2.metric("รายการดิบจากทุกแหล่ง", f"{len(offers):,}")
    k3.metric("ราคาเฉลี่ยในไทย", f"฿{view['best_price_thb'].mean():,.0f}" if view["best_price_thb"].notna().any() else "-")
    best = view.dropna(subset=["thb_per_1000_mark"]).nsmallest(1, "thb_per_1000_mark")
    k4.metric("คุ้มที่สุด (บาท/1000 คะแนน)", best["model_key"].iloc[0] if not best.empty else "-",
              f"฿{best['thb_per_1000_mark'].iloc[0]:,.0f}" if not best.empty else None)

    c1, c2 = st.columns(2)
    with c1:
        scatter = view.dropna(subset=["best_price_thb", "cpu_mark"])
        if not scatter.empty:
            fig = px.scatter(scatter, x="best_price_thb", y="cpu_mark", color="brand", hover_name="model_key",
                             symbol="family", title="ราคาในไทย vs คะแนน CPU Mark",
                             labels={"best_price_thb": "ราคาต่ำสุดในไทย (บาท)", "cpu_mark": "PassMark CPU Mark"},
                             color_discrete_map={"Intel": "#0071C5", "AMD": "#ED1C24"})
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        by_family = view.dropna(subset=["best_price_thb"]).groupby(["brand", "family"], as_index=False)["best_price_thb"].mean()
        if not by_family.empty:
            fig = px.bar(by_family.sort_values("best_price_thb"), x="family", y="best_price_thb", color="brand",
                         title="ราคาเฉลี่ยแยกตามตระกูล", labels={"best_price_thb": "ราคาเฉลี่ย (บาท)", "family": "ตระกูล"},
                         color_discrete_map={"Intel": "#0071C5", "AMD": "#ED1C24"})
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("ตารางรุ่น CPU (1 แถวต่อ 1 รุ่น)")
    st.dataframe(view[["model_key", "brand", "family", "generation", "cpu_mark", "best_price_thb", "avg_price_thb",
                       "passmark_price_usd", "thb_per_1000_mark", "sources", "listing_count"]],
                 use_container_width=True, hide_index=True)

    with st.expander("รายการดิบจากแต่ละแหล่ง (cpu_offers)"):
        st.dataframe(offers[["source", "model_key", "product_name", "price_min_thb", "price_max_thb", "currency",
                             "cpu_mark", "offer_count", "url", "updated_at"]], use_container_width=True, hide_index=True)

# ------------------------------------------------------------------ pipeline status (โบนัส)
else:
    st.title("สถานะ Pipeline (cpu_price_pipeline)")
    runs = safe_query("SELECT * FROM pipeline_runs ORDER BY finished_at DESC NULLS LAST LIMIT 20")
    if runs.empty:
        st.info("ยังไม่มีประวัติการรัน")
        st.stop()
    last = runs.iloc[0]
    icon = {"success": "🟢", "partial": "🟡", "quality_failed": "🔴"}.get(last["status"], "⚪")
    a, b, c, d = st.columns(4)
    a.metric("สถานะล่าสุด", f"{icon} {last['status']}")
    b.metric("แถวที่ผ่านคุณภาพ", f"{int(last['valid_rows'] or 0):,} / {int(last['total_rows'] or 0):,}")
    c.metric("จำนวนรุ่น", f"{int(last['model_count'] or 0):,}")
    d.metric("USD→THB", f"{float(last['usd_thb'] or 0):.2f}")
    st.caption(f"รันล่าสุด: {last['finished_at']} · {last['message']}")

    st.subheader("สถานะแต่ละแหล่งในรอบล่าสุด")
    status = last["source_status"] or {}
    st.dataframe(pd.DataFrame([{"แหล่ง": k, "สถานะ": ("✅ " if v["status"] == "ok" else "❌ ") + v["status"], "จำนวนแถว": v["rows"]}
                               for k, v in status.items()]), hide_index=True)

    st.subheader("ประวัติการรัน")
    st.dataframe(runs[["run_id", "status", "quality_passed", "valid_rows", "total_rows", "model_count", "message", "finished_at"]],
                 use_container_width=True, hide_index=True)

    st.subheader("ตารางกักกัน (ไม่ผ่านกฎคุณภาพ) ล่าสุด")
    st.dataframe(safe_query("SELECT source, product_name, reason, created_at FROM cpu_quarantine ORDER BY created_at DESC LIMIT 50"),
                 use_container_width=True, hide_index=True)
