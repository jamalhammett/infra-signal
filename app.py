import os

import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

if "DATABASE_URL" in st.secrets:
    DATABASE_URL = st.secrets["DATABASE_URL"]
else:
    DATABASE_URL = os.getenv("DATABASE_URL")

st.set_page_config(
    page_title="Infrastructure Intelligence Platform",
    layout="wide"
)


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

st.title("Infrastructure Intelligence Platform")
st.caption("Allen Hammett AI — Private Access Preview")

st.success("🟢 LIVE OPPORTUNITIES: Active infrastructure projects in pre-construction window (0–6 months)")

if not DATABASE_URL:
    st.error("DATABASE_URL not found")
    st.stop()


def run_query(sql: str) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df


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

# Safely normalize timestamps to UTC
df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)


def classify_stage(stage: str) -> str:
    stage = (stage or "").strip()

    if stage in ["Approved", "In Review"]:
        return "🟢 Act Now"
    if stage == "Planning Correspondence":
        return "🟡 Position"
    return "🔵 Track"


def action_text(stage: str) -> str:
    stage = (stage or "").strip()

    if stage in ["Approved", "In Review"]:
        return "Engage immediately — vendor selection likely active"
    if stage == "Planning Correspondence":
        return "Build relationship — early positioning phase"
    return "Monitor only — long-term opportunity"


def freshness_label(date_value) -> str:
    if pd.isnull(date_value):
        return "Unknown"

    try:
        ts = pd.Timestamp(date_value)

        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        now_utc = pd.Timestamp.now(tz="UTC")
        days = (now_utc - ts).days

        if days <= 90:
            return "🟢 Immediate"
        if days <= 180:
            return "🟡 Near-Term"
        return "🔵 Long-Term"
    except Exception:
        return "Unknown"


df["Opportunity Stage"] = df["project_stage"].apply(classify_stage)
df["Recommended Action"] = df["project_stage"].apply(action_text)
df["Timing"] = df["created_at"].apply(freshness_label)

st.header("🟢 Immediate Opportunity Signals")

top_df = df[df["Opportunity Stage"] == "🟢 Act Now"].head(5)

if top_df.empty:
    st.info("No immediate opportunities found in the current filtered set.")
else:
    for _, row in top_df.iterrows():
        project_name = row.get("project_name", "Unknown Project")
        project_stage = row.get("project_stage", "Unknown Stage")
        recommended_action = row.get("Recommended Action", "Review project")

        st.success(
            f"{project_name} ({project_stage}) — {recommended_action}"
        )

st.header("Priority Infrastructure Targets")

display_df = df[
    [
        "case_number",
        "project_name",
        "project_type",
        "project_stage",
        "Opportunity Stage",
        "Timing",
        "Recommended Action",
    ]
].copy()

st.dataframe(display_df, use_container_width=True)

st.header("Project Detail")

project_options = (
    df["project_name"]
    .dropna()
    .astype(str)
    .unique()
    .tolist()
)

if not project_options:
    st.info("No projects available for detail view.")
else:
    selected = st.selectbox("Select a project", project_options)

    detail = df[df["project_name"] == selected].iloc[0]

    st.subheader(str(detail.get("project_name", "Unknown Project")))
    st.write(f"**Stage:** {detail.get('project_stage', 'Unknown')}")
    st.write(f"**Timing:** {detail.get('Timing', 'Unknown')}")
    st.write(f"**Recommended Action:** {detail.get('Recommended Action', 'Review manually')}")

    st.markdown("### Description")
    st.write(detail.get("project_description", ""))

st.info("Allen Hammett AI — Infrastructure Intelligence | Confidential Preview")
