import os
import re

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
     or project_description ilike '%warehouse%'
     or project_name ilike '%cyrus%'
     or project_name ilike '%vantage%'
     or project_name ilike '%intergate%'
     or project_name ilike '%novec%'
     or project_name ilike '%digital realty%'
     or project_name ilike '%qts%'
     or project_name ilike '%stack%'
     or project_name ilike '%switch%'
     or project_name ilike '%coresite%'
     or project_name ilike '%align%'
  )
order by created_at desc
limit 100
""")

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


def clean_text(value) -> str:
    if value is None or pd.isnull(value):
        return ""
    return str(value).strip()


def extract_target_company(project_name: str, description: str) -> str:
    name = clean_text(project_name)
    desc = clean_text(description)

    known_companies = [
        "CyrusOne",
        "Vantage Data Centers",
        "Vantage",
        "QTS",
        "Digital Realty",
        "STACK",
        "Switch",
        "CoreSite",
        "Aligned",
        "NOVEC",
        "Intergate",
        "LC3",
    ]

    combined = f"{name} {desc}"

    for company in known_companies:
        if company.lower() in combined.lower():
            return company

    patterns = [
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\s(?:LLC|Inc|Corp|Corporation|LP|LLP|Ltd))\b",
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\sData Center(?:s)?)\b",
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\sSubstation)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, combined)
        if match:
            return match.group(1).strip()

    if name:
        return name

    return "Unknown Target"


def infer_target_role(project_type: str, project_stage: str, description: str) -> str:
    ptype = clean_text(project_type).lower()
    stage = clean_text(project_stage).lower()
    desc = clean_text(description).lower()

    if "substation" in desc:
        return "Utility / Power Infrastructure Lead"

    if stage in ["approved", "in review"]:
        if "performance bond" in ptype:
            return "General Contractor / Construction Manager"
        if "planning correspondence" in ptype:
            return "Developer / Project Delivery Lead"
        if "legislative application" in ptype:
            return "Developer / Real Estate Lead"
        return "Developer / Construction Lead"

    if "planning" in ptype:
        return "Developer / Design Team"

    return "Developer / Owner Representative"


def why_this_matters(project_type: str, project_stage: str, project_name: str, description: str) -> str:
    ptype = clean_text(project_type).lower()
    stage = clean_text(project_stage).lower()
    name = clean_text(project_name).lower()
    desc = clean_text(description).lower()

    if "substation" in desc or "substation" in name:
        return "Power infrastructure activity often signals major site readiness and downstream construction spend."

    if "performance bond" in ptype:
        return "Performance bonds usually indicate late pre-construction activity and a near-term field execution window."

    if stage == "approved":
        return "Approved status suggests the project is advancing and vendor positioning may already be underway."

    if stage == "in review":
        return "In-review status signals active movement and creates a window to influence specifications before awards are finalized."

    if "planning correspondence" in ptype:
        return "Planning activity indicates project shaping is still underway and relationship-building can influence downstream demand."

    return "This project shows active infrastructure movement with potential downstream procurement value."


def recommended_move(target_role: str, project_stage: str, project_type: str, description: str) -> str:
    role = clean_text(target_role).lower()
    stage = clean_text(project_stage).lower()
    ptype = clean_text(project_type).lower()
    desc = clean_text(description).lower()

    if "utility" in role:
        return "Approach utility and site-infrastructure stakeholders around power readiness, trenching, site access, and contractor support needs."

    if "general contractor" in role or "construction manager" in role:
        return "Reach the GC / CM now to position DeWalt as the preferred jobsite standard before field mobilization ramps."

    if "developer" in role and stage in ["approved", "in review"]:
        return "Reach project delivery leadership now to identify delivery partners, procurement timing, and tool standardization opportunities."

    if "design team" in role:
        return "Start early with the design and development team so DeWalt is visible before execution partners are locked."

    if "owner representative" in role:
        return "Use this as a strategic account signal and map the owner-side decision path before contractor awards."

    return "Use this project as an early outreach opportunity and identify the delivery lead before competitor relationships are entrenched."


df["Opportunity Stage"] = df["project_stage"].apply(classify_stage)
df["Recommended Action"] = df["project_stage"].apply(action_text)
df["Timing"] = df["created_at"].apply(freshness_label)
df["Target Company"] = df.apply(
    lambda row: extract_target_company(row.get("project_name"), row.get("project_description")),
    axis=1
)
df["Target Role"] = df.apply(
    lambda row: infer_target_role(row.get("project_type"), row.get("project_stage"), row.get("project_description")),
    axis=1
)
df["Why This Matters"] = df.apply(
    lambda row: why_this_matters(
        row.get("project_type"),
        row.get("project_stage"),
        row.get("project_name"),
        row.get("project_description")
    ),
    axis=1
)
df["Recommended Move"] = df.apply(
    lambda row: recommended_move(
        row.get("Target Role"),
        row.get("project_stage"),
        row.get("project_type"),
        row.get("project_description")
    ),
    axis=1
)

st.header("🟢 Immediate Opportunity Signals")

top_df = df[df["Opportunity Stage"] == "🟢 Act Now"].head(5)

if top_df.empty:
    st.info("No immediate opportunities found in the current filtered set.")
else:
    for _, row in top_df.iterrows():
        st.success(
            f"{row['Target Company']} — {row['project_name']} ({row['project_stage']}) | Target: {row['Target Role']}"
        )

st.header("Priority Infrastructure Targets")

display_df = df[
    [
        "case_number",
        "Target Company",
        "project_name",
        "project_type",
        "project_stage",
        "Opportunity Stage",
        "Timing",
        "Target Role",
        "Recommended Move",
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
    st.write(f"**Target Company:** {detail.get('Target Company', 'Unknown')}")
    st.write(f"**Target Role:** {detail.get('Target Role', 'Unknown')}")
    st.write(f"**Stage:** {detail.get('project_stage', 'Unknown')}")
    st.write(f"**Timing:** {detail.get('Timing', 'Unknown')}")
    st.write(f"**Opportunity Stage:** {detail.get('Opportunity Stage', 'Unknown')}")

    st.markdown("### Why This Matters")
    st.write(detail.get("Why This Matters", ""))

    st.markdown("### Recommended Move")
    st.write(detail.get("Recommended Move", ""))

    st.markdown("### Description")
    st.write(detail.get("project_description", ""))

st.info("Allen Hammett AI — Infrastructure Intelligence | Confidential Preview")
