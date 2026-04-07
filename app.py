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
st.info(
    "This preview surfaces early-stage land control and infrastructure development signals before traditional procurement visibility."
)

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

st.dataframe(health_df, use_container_width=True)

st.header("High-Value Infrastructure Watchlist")

watchlist_df = run_query("""
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
  and (
        project_type ilike '%industrial%'
     or project_type ilike '%commercial%'
     or project_type ilike '%site%'
     or description ilike '%data center%'
     or description ilike '%datacenter%'
     or description ilike '%cloud%'
     or description ilike '%server%'
     or description ilike '%substation%'
     or description ilike '%warehouse%'
  )
order by opportunity_score desc, created_at desc
limit 50
""")

st.dataframe(watchlist_df, use_container_width=True)

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

st.dataframe(top_projects_df, use_container_width=True)

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

st.dataframe(landholders_df, use_container_width=True)

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

st.dataframe(approved_df, use_container_width=True)

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

st.dataframe(review_df, use_container_width=True)

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
    st.dataframe(signals_df, use_container_width=True)

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
