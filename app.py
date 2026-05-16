import os
import pandas as pd
import psycopg2 as psycopg
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = st.secrets.get(
    "DATABASE_URL",
    os.getenv("DATABASE_URL")
)

st.set_page_config(
    page_title="Infrastructure Intelligence Platform",
    layout="wide"
)


# =====================================
# DATABASE
# =====================================

def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)

    try:
        return pd.read_sql(sql, conn, params=params)

    finally:
        conn.close()


# =====================================
# PROJECTS
# =====================================

def get_projects():

    sql = """
    select
        case_number,
        filing_year,
        project_name,
        project_stage,
        target_company,
        created_at
    from projects
    where created_at >= now() - interval '90 days'
    order by created_at desc
    limit 500
    """

    return run_query(sql)


# =====================================
# LEADS
# =====================================

def get_leads():

    sql = """
    select
        company,
        contact_name,
        title,
        email,
        phone,
        created_at
    from leads
    order by created_at desc
    limit 100
    """

    return run_query(sql)


# =====================================
# PAGE HEADER
# =====================================

st.title("Infrastructure Intelligence Platform")

st.markdown(
    """
Allen Hammett AI — Private Infrastructure / Data Center Intelligence
"""
)

# =====================================
# PROJECTS SECTION
# =====================================

projects_df = get_projects()

st.markdown("## Top Infrastructure Opportunities")

st.dataframe(
    projects_df,
    use_container_width=True
)

# =====================================
# LEADS SECTION
# =====================================

st.markdown("## Infrastructure Leads")

leads_df = get_leads()

st.dataframe(
    leads_df,
    use_container_width=True
)

# =====================================
# EXPORTS
# =====================================

st.markdown("## Export Leads")

csv = leads_df.to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download Leads CSV",
    data=csv,
    file_name="infrastructure_leads.csv",
    mime="text/csv"
)

# =====================================
# FOOTER
# =====================================

st.caption(
    "Allen Hammett AI • Infrastructure Intelligence System"
)
