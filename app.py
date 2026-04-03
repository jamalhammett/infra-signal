import os
import subprocess

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
    page_title="InfraSignal",
    layout="wide"
)
st.title("InfraSignal")
st.caption("Early Infrastructure Intelligence Platform")

if not DATABASE_URL:
    st.error("DATABASE_URL not found in .env")
    st.stop()


def run_query(sql: str) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df


def run_script(command: list[str]) -> tuple[str, str]:
    result = subprocess.run(command, capture_output=True, text=True)
    return result.stdout, result.stderr


# -----------------------------
# ACTION BAR
# -----------------------------
st.subheader("Pipeline Controls")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("Ingest Loudoun API Signals", use_container_width=True):
        stdout, stderr = run_script(["python", r"scripts\generate_signals_from_api.py"])
        if stdout:
            st.text(stdout)
        if stderr:
            st.error(stderr)

with col2:
    if st.button("Promote Signals to Projects", use_container_width=True):
        stdout, stderr = run_script(["python", r"scripts\promote_signals_to_projects.py"])
        if stdout:
            st.text(stdout)
        if stderr:
            st.error(stderr)

with col3:
    if st.button("Run Full Loudoun Pipeline", use_container_width=True):
        steps = [
            ["python", r"scripts\generate_signals_from_api.py"],
            ["python", r"scripts\promote_signals_to_projects.py"],
        ]

        full_output = []

        for step in steps:
            stdout, stderr = run_script(step)
            full_output.append(f"$ {' '.join(step)}")
            if stdout:
                full_output.append(stdout)
            if stderr:
                full_output.append("ERROR:")
                full_output.append(stderr)

        st.text("\n".join(full_output))


# -----------------------------
# PIPELINE HEALTH
# -----------------------------
st.subheader("Pipeline Health")

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

st.dataframe(health_df, use_container_width=True)


# -----------------------------
# TOP OPPORTUNITIES
# -----------------------------
st.header("Top Opportunities")

top_projects_df = run_query("""
select
    case_number,
    canonical_project_name,
    project_type,
    project_stage,
    opportunity_score,
    county,
    state
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
  and project_type not ilike '%Agricultural%'
order by opportunity_score desc, created_at desc
limit 50
""")

st.dataframe(top_projects_df, use_container_width=True)


# -----------------------------
# APPROVED OPPORTUNITIES
# -----------------------------
st.header("Approved Opportunities")

approved_df = run_query("""
select
    case_number,
    canonical_project_name,
    project_type,
    project_stage,
    opportunity_score,
    description
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
  and project_stage = 'Approved'
  and project_type not ilike '%Agricultural%'
order by opportunity_score desc, created_at desc
limit 50
""")

st.dataframe(approved_df, use_container_width=True)


# -----------------------------
# IN REVIEW PIPELINE
# -----------------------------
st.header("In Review Pipeline")

review_df = run_query("""
select
    case_number,
    canonical_project_name,
    project_type,
    project_stage,
    opportunity_score,
    description
from projects
where case_number is not null
  and canonical_project_name <> 'Test Signal'
  and project_stage = 'In Review'
  and project_type not ilike '%Agricultural%'
order by opportunity_score desc, created_at desc
limit 50
""")

st.dataframe(review_df, use_container_width=True)


# -----------------------------
# PROJECT TYPE BREAKDOWN
# -----------------------------
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

st.dataframe(project_type_df, use_container_width=True)


# -----------------------------
# PROJECT STAGE BREAKDOWN
# -----------------------------
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

st.dataframe(project_stage_df, use_container_width=True)


# -----------------------------
# RECENT PROJECTS
# -----------------------------
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

st.dataframe(recent_projects_df, use_container_width=True)


# -----------------------------
# RECENT SIGNALS
# -----------------------------
st.header("Recent Signals")

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

st.dataframe(signals_df, use_container_width=True)


# -----------------------------
# RAW SOURCE MONITORING
# -----------------------------
with st.expander("Source Monitoring", expanded=False):
    st.subheader("Recent Source Runs")
    runs_df = run_query("""
    select id, status, run_started_at, run_finished_at, records_found, records_inserted
    from source_runs
    order by run_started_at desc
    limit 20
    """)
    st.dataframe(runs_df, use_container_width=True)

    st.subheader("Recent Raw Documents")
    docs_df = run_query("""
    select id, document_url, storage_path, fetched_at
    from raw_documents
    order by fetched_at desc
    limit 20
    """)
    st.dataframe(docs_df, use_container_width=True)

    st.subheader("Review Queue")
    reviews_df = run_query("""
    select r.id, r.review_status, r.created_at, d.storage_path
    from raw_snapshot_reviews r
    left join raw_documents d on r.raw_document_id = d.id
    order by r.created_at desc
    limit 20
    """)
    st.dataframe(reviews_df, use_container_width=True)
