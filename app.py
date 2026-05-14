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

st.set_page_config(page_title="Infrastructure Intelligence Platform", layout="wide")


# =========================
# DATABASE
# =========================
def run_query(sql: str):
    conn = psycopg.connect(DATABASE_URL)
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


# =========================
# ADMIN REFRESH
# =========================
def admin_refresh_controls():
    if st.sidebar.button("Refresh Infrastructure Signals"):
        st.sidebar.info("Refresh started...")

        env = os.environ.copy()
        env["DATABASE_URL"] = DATABASE_URL

        try:
            subprocess.run(
                [sys.executable, "scripts/generate_signals_from_api.py"],
                check=True,
                timeout=180,
                env=env
            )

            subprocess.run(
                [sys.executable, "scripts/promote_signals_to_projects.py"],
                check=True,
                timeout=180,
                env=env
            )

            st.sidebar.success("Signals refreshed successfully.")

        except Exception as e:
            st.sidebar.error(f"Refresh failed: {e}")


# =========================
# DATA LOAD ✅ FIXED
# =========================
df = run_query("""
SELECT
    case_number,
    project_name,
    project_type,
    project_stage,
    created_at,
    project_description
FROM signals
WHERE (
    created_at >= NOW() - INTERVAL '90 days'
)
AND (
    LOWER(project_name) LIKE '%data center%'
    OR LOWER(project_description) LIKE '%data center%'
    OR LOWER(project_type) LIKE '%data center%'

    OR LOWER(project_description) LIKE '%substation%'
    OR LOWER(project_description) LIKE '%switchyard%'
    OR LOWER(project_description) LIKE '%server%'
    OR LOWER(project_description) LIKE '%cloud%'
    OR LOWER(project_description) LIKE '%transmission%'
)
AND (
    LOWER(project_description) LIKE '%loudoun%'
    OR LOWER(project_description) LIKE '%prince william%'
    OR LOWER(project_description) LIKE '%fairfax%'
    OR LOWER(project_description) LIKE '%stafford%'
    OR LOWER(project_description) LIKE '%fauquier%'
    OR LOWER(project_description) LIKE '%henrico%'
)
ORDER BY created_at DESC
LIMIT 1500
""")

# =========================
# HELPERS
# =========================
def extract_filing_year(case_number):
    match = re.search(r"(20\d{2}|19\d{2})", str(case_number))
    return match.group(1) if match else "Unknown"


# =========================
# PROCESSING
# =========================
df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
df["Filing Year"] = df["case_number"].apply(extract_filing_year)

df["Target Company"] = df["project_name"]
df["Target Role"] = "Developer"

# =========================
# SCORING ✅ SAFE VERSION
# =========================
def score(row):
    text = f"{row['project_name']} {row['project_description']}".lower()
    score = 0

    if "data center" in text:
        score += 50

    if "substation" in text or "transmission" in text:
        score += 30

    if pd.notnull(row["created_at"]):
        days = (pd.Timestamp.now(tz="UTC") - row["created_at"]).days
        if days <= 90:
            score += 20

    return score


df["Actionability Score"] = df.apply(score, axis=1)

# ✅ FILTER HIGH VALUE ONLY
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

admin_refresh_controls()

st.sidebar.success(
    f"Last Auto Refresh: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}"
)

st.subheader("Top Infrastructure Opportunities")

st.dataframe(df[
    [
        "case_number",
        "Filing Year",
        "project_name",
        "project_stage",
        "Target Company",
        "Target Role",
        "Actionability Score",
        "Outreach Hook"
    ]
])

st.download_button(
    "Download BD Target List",
    data=df.to_csv(index=False),
    file_name="bd_targets.csv",
    mime="text/csv"
)
