import os
import pandas as pd
import psycopg2 as psycopg
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))

st.set_page_config(
    page_title="Infrastructure Intelligence Platform",
    layout="wide"
)

def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

def get_columns(table_name):
    sql = """
    select column_name
    from information_schema.columns
    where table_schema = 'public'
      and table_name = %s
    """
    df = run_query(sql, (table_name,))
    return set(df["column_name"].tolist())

def get_projects():
    cols = get_columns("projects")

    wanted = [
        "case_number",
        "project_name",
        "canonical_project_name",
        "project_stage",
        "project_type",
        "county",
        "state",
        "created_at",
    ]

    select_cols = [c for c in wanted if c in cols]

    if not select_cols:
        return pd.DataFrame()

    sql = f"""
    select {", ".join(select_cols)}
    from projects
    order by created_at desc
    limit 500
    """

    df = run_query(sql)

    if "created_at" in df.columns:
        df = df[pd.to_datetime(df["created_at"], errors="coerce") >= pd.Timestamp.utcnow() - pd.Timedelta(days=90)]

    return df

def get_leads():
    cols = get_columns("leads")

    wanted = [
        "company",
        "contact_name",
        "title",
        "email",
        "phone",
        "created_at",
    ]

    select_cols = [c for c in wanted if c in cols]

    if not select_cols:
        return pd.DataFrame()

    sql = f"""
    select {", ".join(select_cols)}
    from leads
    order by created_at desc
    limit 100
    """

    return run_query(sql)

st.title("Infrastructure Intelligence Platform")
st.markdown("Allen Hammett AI — Private Infrastructure / Data Center Intelligence")

projects_df = get_projects()

st.markdown("## Top Infrastructure Opportunities")
st.dataframe(projects_df, use_container_width=True)

leads_df = get_leads()

st.markdown("## Infrastructure Leads")
st.dataframe(leads_df, use_container_width=True)

st.markdown("## Export Leads")

if not leads_df.empty:
    csv = leads_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download Leads CSV",
        data=csv,
        file_name="infrastructure_leads.csv",
        mime="text/csv"
    )
else:
    st.info("No leads available to export.")

st.caption("Allen Hammett AI • Infrastructure Intelligence System")
