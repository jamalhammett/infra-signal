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
    layout="wide",
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


def run_query(sql: str) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df


def clean_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def extract_target_account(project_name: str, description: str) -> str:
    """
    Try to identify the most likely target account / company / project entity.
    """
    project_name = clean_text(project_name)
    description = clean_text(description)

    known_targets = [
        "CyrusOne",
        "Cyxtera",
        "QTS",
        "Equinix",
        "Digital Realty",
        "Vantage",
        "Compass Datacenters",
        "Stack Infrastructure",
        "Aligned",
        "CoreSite",
        "NTT",
        "Cologix",
        "Amazon",
        "AWS",
        "Microsoft",
        "Google",
        "Meta",
        "Oracle",
        "Novec",
        "Dominion",
        "Intergate",
        "Belmont",
        "Brambleton",
        "Zebra",
        "LC3",
    ]

    haystack = f"{project_name} {description}".lower()

    for target in known_targets:
        if target.lower() in haystack:
            return target

    if ":" in description:
        left = description.split(":", 1)[0].strip()
        if left:
            return left

    if project_name:
        return project_name

    return "Unknown"


def generate_signal_text(project_name: str, project_type: str, description: str) -> str:
    text = f"{clean_text(project_name)} {clean_text(project_type)} {clean_text(description)}".lower()

    if "data center" in text or "datacenter" in text:
        return "Data center development or expansion activity detected"
    if "substation" in text:
        return "Power infrastructure expansion signal detected"
    if "warehouse" in text:
        return "Industrial / logistics development signal detected"
    if "engineering plan" in text:
        return "Engineering-stage infrastructure activity detected"
    if "planning correspondence" in text:
        return "Pre-procurement planning signal detected"
    if "performance bond" in text:
        return "Execution-stage project bonding signal detected"
    if "legislative application" in text or "rezoning" in text:
        return "Entitlement / rezoning activity detected"

    return "General infrastructure activity detected"


def infer_action(project_type: str, project_stage: str, description: str) -> str:
    text = f"{clean_text(project_type)} {clean_text(project_stage)} {clean_text(description)}".lower()

    if "data center" in text or "datacenter" in text:
        if "approved" in text:
            return "Engage immediately: likely opening for power tools, electrical, safety, and site support procurement."
        if "in review" in text or "pending" in text:
            return "Early engagement window: map decision-makers, distributors, and contractors before approval."
        return "Track closely: potential hyperscale or colocation infrastructure opportunity."

    if "substation" in text or "transmission" in text or "utility" in text:
        return "Engage utilities, EPC firms, and electrical contractors around field execution and infrastructure support."

    if "warehouse" in text or "industrial" in text or "commercial" in text:
        return "Identify developer, general contractor, and local distributor opportunities."

    if "site" in text or "engineering plan" in text or "planning correspondence" in text:
        return "Pre-procurement signal: engage before traditional purchasing visibility."

    if "performance bond" in text:
        return "Execution-stage signal: contractor activity is closer to spend and field mobilization."

    if "approved" in text:
        return "Approved project: vendor conversations likely becoming actionable."

    if "in review" in text or "pending" in text:
        return "Monitor actively: this may move into procurement once approvals clear."

    return "Review manually: signal is relevant, but next action needs qualification."


def infer_why_it_matters(project_type: str, project_stage: str, description: str) -> str:
    text = f"{clean_text(project_type)} {clean_text(project_stage)} {clean_text(description)}".lower()

    if "data center" in text or "datacenter" in text:
        return "Data center projects drive large-scale demand for tools, electrical infrastructure support, safety equipment, and contractor relationships."

    if "substation" in text or "utility" in text or "transmission" in text:
        return "Power infrastructure signals often indicate adjacent construction and field-equipment demand."

    if "warehouse" in text or "industrial" in text:
        return "Industrial projects can create repeat demand across site prep, build-out, maintenance, and contractor supply channels."

    if "performance bond" in text:
        return "Performance bond activity often indicates a project is moving closer to execution and real spend."

    if "approved" in text:
        return "Approved status usually means the project is further along and closer to vendor engagement."

    if "in review" in text or "pending" in text:
        return "In-review activity creates early access before broader sales teams typically see the opportunity."

    return "This record may represent a meaningful early-stage development or land-control signal."


def extract_entities(project_name: str, description: str) -> list[str]:
    text = f"{clean_text(project_name)}. {clean_text(description)}"

    patterns = [
        r"\b(?:CyrusOne|Cyxtera|QTS|Equinix|Digital Realty|Amazon|AWS|Microsoft|Meta|Google|Compass Datacenters|Vantage|Aligned|Stack Infrastructure|Sabey|CoreSite|NTT|Switch|Cologix|Oracle|Apple|Novec|Dominion)\b",
        r"\b[A-Z][A-Z0-9&.\- ]{2,}(?:LLC|LP|INC|CORP|CORPORATION|COMPANY|HOLDINGS|VENTURES|GROUP|PARTNERS|PARTNERSHIP)\b",
        r"\b[A-Z][A-Za-z0-9&.\- ]+(?:Data Center|Datacenter|Campus|Innovation|Substation|Rezoning|Industrial Park|Business Park)\b",
        r"\b[A-Z]{2,6}-\d{4}-\d{3,5}\b",
    ]

    found = []

    for pattern in patterns:
        matches = re.findall(pattern, text, flags=re.IGNORECASE)
        for m in matches:
            value = m.strip()
            if value and value not in found:
                found.append(value)

    if project_name and project_name not in found:
        found.insert(0, project_name)

    cleaned = []
    seen = set()

    for item in found:
        item = re.sub(r"\s+", " ", item).strip(" -,:;.")
        if len(item) < 3:
            continue
        key = item.lower()
        if key not in seen:
            seen.add(key)
            cleaned.append(item)

    return cleaned[:10]


def build_outreach_note(
    target_account: str,
    project_name: str,
    project_type: str,
    project_stage: str,
    county: str,
    state: str,
) -> str:
    return (
        f"Hi [Name], we are tracking early-stage infrastructure activity in {county}, {state}, "
        f"including {project_name} tied to {target_account} "
        f"({project_type}, {project_stage}). "
        f"We believe there may be an opportunity to support field execution, contractor productivity, "
        f"electrical infrastructure work, and site build-out. "
        f"I’d welcome a short conversation to understand where vendor engagement stands."
    )


check_login()

st.title("Infrastructure Intelligence Platform")
st.caption("Allen Hammett AI — Private Access Preview")
st.info(
    "This preview surfaces early-stage land control and infrastructure development signals before traditional procurement visibility."
)

if not DATABASE_URL:
    st.error("DATABASE_URL not found")
    st.stop()

st.header("🚨 Active Infrastructure Signals")
st.warning(
    "LC3 Data Center: Amendment detected in Loudoun County — potential expansion and vendor engagement window."
)
st.warning(
    "Data center and industrial signals are being surfaced ahead of traditional procurement visibility."
)

st.subheader("Platform Health")

health_df = run_query("""
with signal_counts as (
    select count(*) as signals_count from signals
),
project_counts as (
    select count(*) as projects_count from projects
),
latest_signal as (
    select max(created_at) as latest_signal_at from signals
),
latest_project as (
    select max(created_at) as latest_project_at from projects
)
select
    s.signals_count,
    p.projects_count,
    ls.latest_signal_at,
    lp.latest_project_at
from signal_counts s
cross join project_counts p
cross join latest_signal ls
cross join latest_project lp
""")

st.dataframe(health_df, use_container_width=True, hide_index=True)

st.header("Priority Infrastructure Targets")

watchlist_df = run_query("""
select
    case_number,
    canonical_project_name,
    project_type,
    project_stage,
    opportunity_score,
    county,
    state,
    description,
    created_at
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
  and (
        project_type ilike '%industrial%'
     or project_type ilike '%commercial%'
     or project_type ilike '%site%'
     or project_type ilike '%engineering%'
     or project_type ilike '%planning%'
     or project_type ilike '%performance bond%'
     or project_type ilike '%legislative%'
     or description ilike '%data center%'
     or description ilike '%datacenter%'
     or description ilike '%cloud%'
     or description ilike '%server%'
     or description ilike '%substation%'
     or description ilike '%warehouse%'
     or canonical_project_name ilike '%data center%'
     or canonical_project_name ilike '%datacenter%'
     or canonical_project_name ilike '%cyrusone%'
     or canonical_project_name ilike '%vantage%'
     or canonical_project_name ilike '%intergate%'
  )
order by opportunity_score desc, created_at desc
limit 50
""")

display_watchlist_df = watchlist_df[
    [
        "case_number",
        "canonical_project_name",
        "project_type",
        "project_stage",
        "opportunity_score",
        "county",
        "state",
    ]
].copy()

selection = st.dataframe(
    display_watchlist_df,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    key="watchlist_table",
)

selected_row = None
selected_indices = selection.selection.rows if selection and selection.selection else []

if selected_indices:
    selected_row = watchlist_df.iloc[selected_indices[0]]
else:
    if not watchlist_df.empty:
        selected_row = watchlist_df.iloc[0]

if selected_row is not None:
    project_name = clean_text(selected_row["canonical_project_name"])
    project_type = clean_text(selected_row["project_type"])
    project_stage = clean_text(selected_row["project_stage"])
    description = clean_text(selected_row["description"])
    county = clean_text(selected_row["county"])
    state = clean_text(selected_row["state"])
    case_number = clean_text(selected_row["case_number"])
    created_at = selected_row["created_at"]
    opportunity_score = int(selected_row["opportunity_score"])

    target_account = extract_target_account(project_name, description)
    signal_text = generate_signal_text(project_name, project_type, description)
    why_it_matters = infer_why_it_matters(project_type, project_stage, description)
    action_text = infer_action(project_type, project_stage, description)
    entities = extract_entities(project_name, description)
    outreach_note = build_outreach_note(
        target_account=target_account,
        project_name=project_name,
        project_type=project_type,
        project_stage=project_stage,
        county=county,
        state=state,
    )

    st.subheader("Selected Target Detail")

    c1, c2 = st.columns([2, 1])

    with c1:
        st.markdown(f"**🎯 Target Account:** {target_account}")
        st.markdown(f"**🏗 Project:** {project_name}")
        st.markdown(f"**📁 Case Number:** {case_number}")
        st.markdown(f"**📡 Signal:** {signal_text}")
        st.markdown(f"**📍 Location:** {county}, {state}")
        st.markdown(f"**🧱 Project Type:** {project_type}")
        st.markdown(f"**🚦 Stage:** {project_stage}")
        st.markdown(f"**🕒 Loaded:** {created_at}")

    with c2:
        st.metric("Opportunity Score", opportunity_score)

    st.markdown("**Full Description**")
    st.write(description if description else "No description available.")

    st.markdown("**Why This Matters**")
    st.info(why_it_matters)

    st.markdown("**Recommended Action**")
    st.success(action_text)

    st.markdown("**Potential Contacts / Entities**")
    if entities:
        for entity in entities:
            st.write(f"- {entity}")
    else:
        st.write("No likely entities extracted yet.")

    st.markdown("**Suggested Outreach Note**")
    st.code(outreach_note, language=None)

st.header("Top Priority Opportunities")

top_projects_df = run_query("""
select
    case_number,
    canonical_project_name,
    project_type,
    project_stage,
    opportunity_score,
    county,
    state,
    description
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
  and project_type not ilike '%Agricultural%'
order by opportunity_score desc, created_at desc
limit 50
""")

st.dataframe(top_projects_df, use_container_width=True, hide_index=True)

st.header("Top Landholders")

landholders_df = run_query("""
select
    canonical_project_name,
    count(*) as filings,
    count(distinct case_number) as unique_cases,
    min(created_at) as first_seen,
    max(created_at) as last_seen
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
group by canonical_project_name
having count(*) > 1
order by filings desc, last_seen desc
limit 50
""")

st.dataframe(landholders_df, use_container_width=True, hide_index=True)

st.header("Approved Pipeline")

approved_df = run_query("""
select
    case_number,
    canonical_project_name,
    project_type,
    project_stage,
    opportunity_score,
    county,
    state,
    description
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
  and project_stage = 'Approved'
  and project_type not ilike '%Agricultural%'
order by opportunity_score desc, created_at desc
limit 50
""")

st.dataframe(approved_df, use_container_width=True, hide_index=True)

st.header("Active Development Pipeline")

review_df = run_query("""
select
    case_number,
    canonical_project_name,
    project_type,
    project_stage,
    opportunity_score,
    county,
    state,
    description
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
  and project_stage in ('In Review', 'Pending')
  and project_type not ilike '%Agricultural%'
order by opportunity_score desc, created_at desc
limit 50
""")

st.dataframe(review_df, use_container_width=True, hide_index=True)

st.header("Project Type Breakdown")

project_type_df = run_query("""
select
    project_type,
    count(*) as project_count
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
group by project_type
order by project_count desc
limit 25
""")

st.dataframe(project_type_df, use_container_width=True, hide_index=True)

st.header("Project Stage Breakdown")

project_stage_df = run_query("""
select
    project_stage,
    count(*) as project_count
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
group by project_stage
order by project_count desc
""")

st.dataframe(project_stage_df, use_container_width=True, hide_index=True)

st.header("Recent Projects")

recent_projects_df = run_query("""
select
    case_number,
    canonical_project_name,
    project_type,
    project_stage,
    opportunity_score,
    created_at
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
order by created_at desc
limit 100
""")

st.dataframe(recent_projects_df, use_container_width=True, hide_index=True)

with st.expander("Operational Monitoring", expanded=False):
    st.subheader("Recent Signals")
    signals_df = run_query("""
    select
        case_number,
        project_name,
        project_type,
        project_stage,
        confidence_score,
        created_at
    from signals
    where case_number is not null
    order by created_at desc
    limit 100
    """)
    st.dataframe(signals_df, use_container_width=True, hide_index=True)

    st.subheader("Recent Source Runs")
    runs_df = run_query("""
    select id, status, run_started_at, run_finished_at, records_found, records_inserted
    from source_runs
    order by run_started_at desc
    limit 20
    """)
    st.dataframe(runs_df, use_container_width=True, hide_index=True)

    st.subheader("Recent Raw Documents")
    docs_df = run_query("""
    select id, document_url, storage_path, fetched_at
    from raw_documents
    order by fetched_at desc
    limit 20
    """)
    st.dataframe(docs_df, use_container_width=True, hide_index=True)

    st.subheader("Review Queue")
    reviews_df = run_query("""
    select r.id, r.review_status, r.created_at, d.storage_path
    from raw_snapshot_reviews r
    left join raw_documents d on r.raw_document_id = d.id
    order by r.created_at desc
    limit 20
    """)
    st.dataframe(reviews_df, use_container_width=True, hide_index=True)
