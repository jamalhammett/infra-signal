import os
import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

if "DATABASE_URL" in st.secrets:
    DATABASE_URL = st.secrets["DATABASE_URL"]
else:
    DATABASE_URL = os.getenv("DATABASE_URL")

st.set_page_config(
    page_title="Infrastructure Intelligence Platform",
    layout="wide"
)

# ---------------- LOGIN ----------------
def check_login():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.title("Allen Hammett AI")
        st.subheader("Private Infrastructure Intelligence Access")

        password = st.text_input("Enter Access Code", type="password")

        if password == "dewalt2026":
            st.session_state.authenticated = True
            st.rerun()
        elif password:
            st.error("Invalid access code")

        st.stop()

check_login()

# ---------------- HEADER ----------------
st.title("Infrastructure Intelligence Platform")
st.caption("Allen Hammett AI — Private Access Preview")

st.success("🟢 LIVE OPPORTUNITIES: Active infrastructure projects in pre-construction window (0–6 months)")

# ---------------- DB QUERY ----------------
def run_query(sql: str) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df

# ---------------- CORE DATA ----------------
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
        created_at >= now() - interval '180 days'
        or project_stage in ('Approved', 'In Review')
      )
  and (
        project_description ilike '%data center%'
     or project_name ilike '%data center%'
     or project_description ilike '%substation%'
  )
order by created_at desc
limit 100
""")

# 🔥 FIX: Convert created_at to datetime
df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

# ---------------- LOGIC ----------------
def classify_stage(stage):
    if stage in ["Approved", "In Review"]:
        return "🟢 Act Now"
    elif stage in ["Planning Correspondence"]:
        return "🟡 Position"
    else:
        return "🔵 Track"

def action_text(stage):
    if stage in ["Approved", "In Review"]:
        return "Engage immediately — vendor selection likely active"
    elif stage in ["Planning Correspondence"]:
        return "Build relationship — early positioning phase"
    else:
        return "Monitor only — long-term opportunity"

def freshness_label(date):
    if pd.isnull(date):
        return "Unknown"

    days = (datetime.now() - date).days

    if days <= 90:
        return "🟢 Immediate"
    elif days <= 180:
        return "🟡 Near-Term"
    else:
        return "🔵 Long-Term"

# ---------------- ENRICH ----------------
df["Opportunity Stage"] = df["project_stage"].apply(classify_stage)
df["Recommended Action"] = df["project_stage"].apply(action_text)
df["Timing"] = df["created_at"].apply(freshness_label)

# ---------------- TOP SIGNALS ----------------
st.header("🟢 Immediate Opportunity Signals")

top_df = df[df["Opportunity Stage"] == "🟢 Act Now"].head(5)

for _, row in top_df.iterrows():
    st.success(
        f"{row['project_name']} ({row['project_stage']}) — {row['Recommended Action']}"
    )

# ---------------- MAIN TABLE ----------------
st.header("Priority Infrastructure Targets")

display_df = df[
    [
        "case_number",
        "project_name",
        "project_type",
        "project_stage",
        "Opportunity Stage",
        "Timing",
        "Recommended Action"
    ]
]

st.dataframe(display_df, use_container_width=True)

# ---------------- DETAIL ----------------
st.header("Project Detail")

selected = st.selectbox(
    "Select a project",
    df["project_name"].unique()
)

detail = df[df["project_name"] == selected].iloc[0]

st.subheader(detail["project_name"])

st.write(f"**Stage:** {detail['project_stage']}")
st.write(f"**Timing:** {detail['Timing']}")
st.write(f"**Recommended Action:** {detail['Recommended Action']}")

st.markdown("### Description")
st.write(detail["project_description"])

# ---------------- FOOTER ----------------
st.info("Allen Hammett AI — Infrastructure Intelligence | Confidential Preview")
