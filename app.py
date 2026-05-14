# =========================
# IMPORTS
# =========================
import os
import re
import subprocess
import sys

import pandas as pd
import psycopg2 as psycopg
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))
SUPABASE_URL = st.secrets.get("SUPABASE_URL", os.getenv("SUPABASE_URL"))
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY", os.getenv("SUPABASE_ANON_KEY"))

st.set_page_config(page_title="Infrastructure Intelligence Platform", layout="wide")

# =========================
# DATABASE
# =========================
def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# =========================
# ADMIN REFRESH
# =========================
def admin_refresh_controls(role):
    if role != "admin":
        return

    st.sidebar.header("Admin Controls")

    if st.sidebar.button("Refresh Infrastructure Signals"):
        st.sidebar.info("Refresh started...")

        env = os.environ.copy()
        env["DATABASE_URL"] = DATABASE_URL

        try:
            result1 = subprocess.run(
                [sys.executable, "scripts/generate_signals_from_api.py"],
                check=True,
                timeout=180,
                capture_output=True,
                text=True,
                env=env
            )

            result2 = subprocess.run(
                [sys.executable, "scripts/promote_signals_to_projects.py"],
                check=True,
                timeout=180,
                capture_output=True,
                text=True,
                env=env
            )

            st.sidebar.success("Signals refreshed.")
            st.sidebar.code(result1.stdout + "\n" + result2.stdout)

        except subprocess.CalledProcessError as e:
            st.sidebar.error("Refresh failed.")
            st.sidebar.code((e.stdout or "") + "\n" + (e.stderr or ""))

# =========================
# DATA LOAD (UPDATED 🔥)
# =========================
df = run_query("""
select
    case_number,
    project_name,
    project_type,
    project_stage,
    created_at,
    project_description
from signals
where (
    created_at >= now() - interval '90 days'
)
and (
    lower(project_name) like '%data center%'
    or lower(project_description) like '%data center%'
    or lower(project_type) like '%data center%'

    or lower(project_description) like '%substation%'
    or lower(project_description) like '%switchyard%'
    or lower(project_description) like '%server%'
    or lower(project_description) like '%cloud%'
    or lower(project_description) like '%transmission%'
)
and (
    lower(project_description) like '%loudoun%'
    or lower(project_description) like '%prince william%'
    or lower(project_description) like '%fairfax%'
    or lower(project_description) like '%stafford%'
    or lower(project_description) like '%fauquier%'
    or lower(project_description) like '%henrico%'
)
order by created_at desc
limit 1500
""")

# =========================
# HELPERS
# =========================
def extract_filing_year(case_number: str):
    match = re.search(r"(20\d{2}|19\d{2})", str(case_number))
    return match.group(1) if match else "Unknown"

# =========================
# PROCESSING
# =========================
df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
df["Filing Year"] = df["case_number"].apply(extract_filing_year)

# Basic placeholders (your existing logic can stay if you want)
df["Target Company"] = df["project_name"]
df["Target Role"] = "Developer"

# =========================
# SCORING (KEEP SIMPLE + POWERFUL)
# =========================
def score(row):
    score = 0

    text = f"{row['project_name']} {row['project_description']}".lower()

    if "data center" in text:
        score += 50

    if "substation" in text or "transmission" in text:
        score += 30

    if row["created_at"]:
        days = (pd.Timestamp.now(tz="UTC") - row["created_at"]).days
        if days < 90:
            score += 20

    return score

df["Actionability Score"] = df.apply(score, axis=1)

# ✅ CRITICAL FILTER
df = df[df["Actionability Score"] >= 60]

# =========================
# BD OUTPUT
# =========================
df["Opportunity Stage"] = df["Actionability Score"].apply(
    lambda x: "🟢 Act Now" if x >= 80 else "🟡 Position Early"
)

df["Outreach Hook"] = df.apply(
    lambda row: f"Reach out regarding {row['project_name']} ({row['project_stage']})",
    axis=1
)

# =========================
# UI
# =========================
st.title("Infrastructure Intelligence Platform")

# ✅ LAST REFRESH
st.sidebar.success(f"Last Auto Refresh: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")

st.subheader("Top Infrastructure Opportunities")

st.dataframe(df[[
    "case_number",
    "Filing Year",
    "project_name",
    "project_stage",
    "Target Company",
    "Target Role",
    "Actionability Score",
    "Outreach Hook"
]])

# ✅ EXPORT
st.download_button(
    "Download BD Target List",
    data=df.to_csv(index=False),
    file_name="bd_targets.csv",
    mime="text/csv"
)
``
