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

st.title("Infrastructure Intelligence Platform")
st.caption("Allen Hammett AI — Private Access Preview")
st.success("🟢 Action-only mode: showing prioritized infrastructure targets with BD guidance.")

if not DATABASE_URL:
    st.error("DATABASE_URL not found")
    st.stop()


# ---------------- DATA ACCESS ----------------
def run_query(sql: str) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df


# Pull a wider set, then filter/rank in Python
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
        created_at >= now() - interval '365 days'
        or project_stage in ('Approved', 'In Review', 'Pending')
      )
order by created_at desc
limit 1000
""")

df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)


# ---------------- HELPERS ----------------
def clean_text(value) -> str:
    if value is None or pd.isnull(value):
        return ""
    return str(value).strip()


def extract_filing_year(case_number: str) -> str:
    text = clean_text(case_number)
    match = re.search(r"(20\d{2}|19\d{2})", text)
    if match:
        return match.group(1)
    return "Unknown"


def extract_target_company(project_name: str, description: str) -> str:
    name = clean_text(project_name)
    desc = clean_text(description)
    combined = f"{name} {desc}"

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
        "Dominion Energy",
        "BlackChamber Group",
        "Intergate",
        "LC3",
        "Amazon",
        "Meta",
        "Google",
        "Microsoft",
    ]

    for company in known_companies:
        if company.lower() in combined.lower():
            return company

    patterns = [
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\s(?:LLC|Inc|Corp|Corporation|LP|LLP|Ltd))\b",
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\sData Center(?:s)?)\b",
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\sGroup)\b",
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\sEnergy)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, combined)
        if match:
            return match.group(1).strip()

    if name:
        return name

    return "Unknown Target"


def infer_target_role(project_type: str, project_stage: str, description: str, project_name: str) -> str:
    ptype = clean_text(project_type).lower()
    stage = clean_text(project_stage).lower()
    desc = clean_text(description).lower()
    name = clean_text(project_name).lower()
    combined = f"{ptype} {desc} {name}"

    if "substation" in combined or "dominion" in combined or "novec" in combined:
        return "Utility / Power Infrastructure Lead"

    if "performance bond" in combined:
        return "General Contractor / Construction Manager"

    if stage in ["approved", "in review", "pending"]:
        if "data center" in combined:
            return "Developer / Project Delivery Lead"
        if "warehouse" in combined or "industrial" in combined or "commercial" in combined:
            return "Developer / Construction Lead"

    if "planning correspondence" in combined:
        return "Developer / Design Team"

    if "legislative" in combined or "rezoning" in combined:
        return "Developer / Real Estate Lead"

    return "Developer / Owner Representative"


def timing_bucket(date_value) -> str:
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


def days_since(date_value):
    if pd.isnull(date_value):
        return 9999

    try:
        ts = pd.Timestamp(date_value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        now_utc = pd.Timestamp.now(tz="UTC")
        return int((now_utc - ts).days)
    except Exception:
        return 9999


def relevance_score(project_name: str, project_type: str, description: str) -> int:
    name = clean_text(project_name).lower()
    ptype = clean_text(project_type).lower()
    desc = clean_text(description).lower()
    combined = f"{name} {ptype} {desc}"

    score = 0

    high_value_keywords = [
        "data center",
        "datacenter",
        "substation",
        "warehouse",
        "industrial",
        "commercial",
        "cloud",
        "server",
        "switchyard",
        "power",
    ]

    for kw in high_value_keywords:
        if kw in combined:
            score += 20

    if any(x in combined for x in ["cyrus", "vantage", "qts", "digital realty", "stack", "coresite", "aligned", "intergate", "novec", "dominion"]):
        score += 25

    if "agricultural" in combined or "forestal" in combined or "vineyard" in combined:
        score -= 60

    return score


def stage_score(project_stage: str, project_type: str, description: str) -> int:
    stage = clean_text(project_stage).lower()
    ptype = clean_text(project_type).lower()
    desc = clean_text(description).lower()
    combined = f"{stage} {ptype} {desc}"

    score = 0

    if stage == "approved":
        score += 40
    elif stage == "in review":
        score += 30
    elif stage == "pending":
        score += 20

    if "performance bond" in combined:
        score += 35

    if "amendment" in combined:
        score += 20

    return score


def freshness_score(date_value) -> int:
    d = days_since(date_value)

    if d <= 90:
        return 30
    if d <= 180:
        return 20
    if d <= 365:
        return 10
    return 0


def targetability_score(target_company: str, target_role: str, description: str) -> int:
    company = clean_text(target_company).lower()
    role = clean_text(target_role).lower()
    desc = clean_text(description).lower()

    score = 0

    if company and company != "unknown target":
        score += 20

    if role and "unknown" not in role:
        score += 20

    if desc:
        score += 10

    return score


def actionability_score(row) -> int:
    return (
        relevance_score(row["project_name"], row["project_type"], row["project_description"])
        + stage_score(row["project_stage"], row["project_type"], row["project_description"])
        + freshness_score(row["created_at"])
        + targetability_score(row["Target Company"], row["Target Role"], row["project_description"])
    )


def opportunity_stage_from_score(score: int) -> str:
    if score >= 85:
        return "🟢 Act Now"
    if score >= 55:
        return "🟡 Position Early"
    return "🔵 Research Queue"


def why_this_matters(row) -> str:
    project_type = clean_text(row["project_type"]).lower()
    project_stage = clean_text(row["project_stage"]).lower()
    project_name = clean_text(row["project_name"]).lower()
    desc = clean_text(row["project_description"]).lower()
    combined = f"{project_type} {project_stage} {project_name} {desc}"

    if "substation" in combined or "dominion" in combined or "novec" in combined:
        return "Power infrastructure movement often signals major site-readiness and downstream construction spend."

    if "amendment" in combined and "data center" in combined:
        return "A data center amendment often indicates expansion activity and a new vendor influence window."

    if "approved" in combined and "data center" in combined:
        return "Approved data center work suggests the project is moving toward procurement and execution."

    if "in review" in combined and "data center" in combined:
        return "An in-review data center project creates a window to influence stakeholders before award paths harden."

    if "performance bond" in combined:
        return "Performance bond activity is a late pre-construction signal and often precedes field mobilization."

    if "warehouse" in combined or "industrial" in combined or "commercial" in combined:
        return "Industrial and commercial site movement often signals near-term contractor and equipment demand."

    return "This signal shows active infrastructure movement with potential downstream procurement value."


def recommended_move(row) -> str:
    role = clean_text(row["Target Role"]).lower()
    stage = clean_text(row["project_stage"]).lower()
    project_type = clean_text(row["project_type"]).lower()
    desc = clean_text(row["project_description"]).lower()
    combined = f"{role} {stage} {project_type} {desc}"

    if "utility" in role:
        return "Approach power and site-infrastructure stakeholders now around site readiness, contractor activity, and field support needs."

    if "general contractor" in role or "construction manager" in role:
        return "Reach the GC / CM now to position DeWalt as the jobsite standard before mobilization accelerates."

    if "developer / project delivery lead" in role:
        return "Approach developer-side project delivery leadership now to map procurement timing and identify execution partners."

    if "developer / design team" in role:
        return "Begin early relationship-building with the development/design team before final execution partners are locked."

    if "developer / real estate lead" in role:
        return "Treat this as strategic account mapping and identify when the project moves from land control into execution."

    return "Use this signal to identify the delivery-side decision path before competitors establish the relationship."


# ---------------- ENRICH ----------------
df["Filing Year"] = df["case_number"].apply(extract_filing_year)
df["Target Company"] = df.apply(
    lambda row: extract_target_company(row.get("project_name"), row.get("project_description")),
    axis=1
)
df["Target Role"] = df.apply(
    lambda row: infer_target_role(
        row.get("project_type"),
        row.get("project_stage"),
        row.get("project_description"),
        row.get("project_name")
    ),
    axis=1
)
df["Timing"] = df["created_at"].apply(timing_bucket)
df["Days Since Signal"] = df["created_at"].apply(days_since)
df["Actionability Score"] = df.apply(actionability_score, axis=1)
df["Opportunity Stage"] = df["Actionability Score"].apply(opportunity_stage_from_score)
df["Why This Matters"] = df.apply(why_this_matters, axis=1)
df["Recommended Move"] = df.apply(recommended_move, axis=1)

# Remove obvious junk from main flow
junk_mask = (
    df["project_name"].fillna("").str.lower().str.contains("agricultural|forestal|vineyard", regex=True)
    | df["project_type"].fillna("").str.lower().str.contains("agricultural|forestal", regex=True)
    | df["project_description"].fillna("").str.lower().str.contains("agricultural|forestal|vineyard", regex=True)
)
df["Is Junk"] = junk_mask

action_df = df[(df["Is Junk"] == False) & (df["Actionability Score"] >= 55)].copy()
research_df = df[(df["Is Junk"] == False) & (df["Actionability Score"] < 55)].copy()

action_df = action_df.sort_values(
    by=["Actionability Score", "created_at"],
    ascending=[False, False]
)
research_df = research_df.sort_values(
    by=["Actionability Score", "created_at"],
    ascending=[False, False]
)

act_now_df = action_df[action_df["Opportunity Stage"] == "🟢 Act Now"].copy()
position_df = action_df[action_df["Opportunity Stage"] == "🟡 Position Early"].copy()


# ---------------- KPI BAR ----------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Act Now Targets", len(act_now_df))
with col2:
    st.metric("Position Early Targets", len(position_df))
with col3:
    st.metric("Research Queue", len(research_df))
with col4:
    latest_ts = df["created_at"].max()
    latest_label = "Unknown" if pd.isnull(latest_ts) else str(latest_ts.date())
    st.metric("Latest Signal Date", latest_label)


# ---------------- TOP SIGNALS ----------------
st.header("🟢 Immediate Opportunity Signals")

if act_now_df.empty:
    st.info("No immediate targets found in the current filtered set.")
else:
    for _, row in act_now_df.head(8).iterrows():
        st.success(
            f"{row['Target Company']} — {row['project_name']} | {row['project_stage']} | Target: {row['Target Role']}"
        )


# ---------------- ACT NOW ----------------
st.header("🟢 Act Now")

if act_now_df.empty:
    st.info("No act-now targets currently pass the actionability threshold.")
else:
    st.dataframe(
        act_now_df[
            [
                "case_number",
                "Filing Year",
                "Target Company",
                "project_name",
                "project_type",
                "project_stage",
                "Timing",
                "Target Role",
                "Actionability Score",
                "Recommended Move",
            ]
        ],
        use_container_width=True
    )


# ---------------- POSITION EARLY ----------------
st.header("🟡 Position Early")

if position_df.empty:
    st.info("No position-early targets currently pass the actionability threshold.")
else:
    st.dataframe(
        position_df[
            [
                "case_number",
                "Filing Year",
                "Target Company",
                "project_name",
                "project_type",
                "project_stage",
                "Timing",
                "Target Role",
                "Actionability Score",
                "Recommended Move",
            ]
        ],
        use_container_width=True
    )


# ---------------- RESEARCH QUEUE ----------------
with st.expander("🔵 Research Queue (Lower Confidence / Watchlist)", expanded=False):
    if research_df.empty:
        st.info("No research items available.")
    else:
        st.dataframe(
            research_df[
                [
                    "case_number",
                    "Filing Year",
                    "Target Company",
                    "project_name",
                    "project_type",
                    "project_stage",
                    "Timing",
                    "Target Role",
                    "Actionability Score",
                ]
            ].head(100),
            use_container_width=True
        )


# ---------------- DETAIL VIEW ----------------
st.header("Project Detail")

detail_source = action_df if not action_df.empty else df
project_options = (
    detail_source["project_name"]
    .dropna()
    .astype(str)
    .unique()
    .tolist()
)

if not project_options:
    st.info("No projects available for detail view.")
else:
    selected = st.selectbox("Select a project", project_options)
    detail = detail_source[detail_source["project_name"] == selected].iloc[0]

    st.subheader(str(detail.get("project_name", "Unknown Project")))

    left, right = st.columns(2)

    with left:
        st.write(f"**Case Number:** {detail.get('case_number', '')}")
        st.write(f"**Filing Year:** {detail.get('Filing Year', 'Unknown')}")
        st.write(f"**Project Type:** {detail.get('project_type', 'Unknown')}")
        st.write(f"**Project Stage:** {detail.get('project_stage', 'Unknown')}")
        st.write(f"**Timing:** {detail.get('Timing', 'Unknown')}")
        st.write(f"**Opportunity Stage:** {detail.get('Opportunity Stage', 'Unknown')}")
        st.write(f"**Actionability Score:** {detail.get('Actionability Score', 'Unknown')}")

    with right:
        st.write(f"**Target Company:** {detail.get('Target Company', 'Unknown')}")
        st.write(f"**Target Role:** {detail.get('Target Role', 'Unknown')}")
        st.write(f"**Days Since Signal:** {detail.get('Days Since Signal', 'Unknown')}")

    st.markdown("### Why This Matters")
    st.write(detail.get("Why This Matters", ""))

    st.markdown("### Recommended Move")
    st.write(detail.get("Recommended Move", ""))

    st.markdown("### Description")
    st.write(detail.get("project_description", ""))


st.info("Allen Hammett AI — Infrastructure Intelligence | Confidential Preview")
