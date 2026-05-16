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

# ====================================
# DATABASE
# ====================================

def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)

    try:
        return pd.read_sql(sql, conn, params=params)

    finally:
        conn.close()

# ====================================
# LOGIN
# ====================================

def authenticate(email, password):

    sql = """
    select *
    from users
    where email = %s
      and password = %s
    limit 1
    """

    df = run_query(sql, (email, password))

    return not df.empty

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:

    st.title("Allen Hammett AI")

    st.subheader("Secure Infrastructure Intelligence Access")

    email = st.text_input("Email")
    password = st.text_input("Password", type="password")

    login_button = st.button("Login")

    if login_button:

        if authenticate(email, password):

            st.session_state.logged_in = True
            st.success("Login successful")
            st.rerun()

        else:
            st.error("Invalid credentials")

    st.stop()

# ====================================
# PROJECTS
# ====================================

def get_projects():

    sql = """
    select
        case_number,
        canonical_project_name,
        project_stage,
        project_type,
        county,
        state,
        created_at
    from projects
    where created_at >= now() - interval '90 days'
    order by created_at desc
    limit 500
    """

    return run_query(sql)

# ====================================
# LEADS
# ====================================

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

# ====================================
# UI
# ====================================

st.title("Infrastructure Intelligence Platform")

st.markdown(
    "Allen Hammett AI — Private Infrastructure / Data Center Intelligence"
)

projects_df = get_projects()

st.markdown("## Top Infrastructure Opportunities")

st.dataframe(
    projects_df,
    use_container_width=True
)

leads_df = get_leads()

st.markdown("## Infrastructure Leads")

st.dataframe(
    leads_df,
    use_container_width=True
)

# ====================================
# EXPORT
# ====================================

st.markdown("## Export Leads")

if not leads_df.empty:

    csv = leads_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download Leads CSV",
        data=csv,
        file_name="infrastructure_leads.csv",
        mime="text/csv"
    )

st.caption(
    "Allen Hammett AI • Infrastructure Intelligence System"
)
