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

TARGET_KEYWORDS = [
    "data center", "datacenter", "substation", "switchyard",
    "transmission", "utility", "power", "energy", "fiber",
    "telecom", "hyperscale", "cloud", "server", "industrial"
]

EXCLUDED_KEYWORDS = [
    "sidewalk", "trail", "path", "driveway", "subdivision",
    "townhome", "townhomes", "single family", "residential",
    "lot ", "lots", "farm", "vineyard", "conservancy",
    "school", "church", "playground", "park", "landscape",
    "forest", "stormwater", "road widening"
]


def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


def authenticate(email, password):
    df = run_query(
        """
        select email, role
        from users
        where lower(email) = lower(%s)
          and password = %s
        limit 1
        """,
        (email.strip(), password),
    )

    if df.empty:
        return None

    return df.iloc[0].to_dict()


def login_gate():
    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user:
        return st.session_state.user

    st.title("Allen Hammett AI")
    st.subheader("Secure Infrastructure Intelligence Access")

    email = st.text_input("Email")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        user = authenticate(email, password)

        if user:
            st.session_state.user = user
            st.rerun()
        else:
            st.error("Invalid credentials")

    st.stop()


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
    limit 1000
    """

    df = run_query(sql)

    if df.empty:
        return df

    df["combined_text"] = (
        df.fillna("")
        .astype(str)
        .agg(" ".join, axis=1)
        .str.lower()
    )

    target_pattern = "|".join(TARGET_KEYWORDS)
    exclude_pattern = "|".join(EXCLUDED_KEYWORDS)

    df = df[
        df["combined_text"].str.contains(target_pattern, case=False, na=False)
        & ~df["combined_text"].str.contains(exclude_pattern, case=False, na=False)
    ].copy()

    df = df.drop(columns=["combined_text"], errors="ignore")

    return df


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


user = login_gate()

st.title("Infrastructure Intelligence Platform")
st.markdown("Allen Hammett AI — Private Infrastructure / Data Center Intelligence")

projects_df = get_projects()
leads_df = get_leads()

st.sidebar.header("Filters")

if not projects_df.empty:
    counties = ["All"] + sorted(projects_df["county"].dropna().unique().tolist())
    stages = ["All"] + sorted(projects_df["project_stage"].dropna().unique().tolist())
    types = ["All"] + sorted(projects_df["project_type"].dropna().unique().tolist())

    selected_county = st.sidebar.selectbox("County", counties)
    selected_stage = st.sidebar.selectbox("Stage", stages)
    selected_type = st.sidebar.selectbox("Project Type", types)

    filtered_df = projects_df.copy()

    if selected_county != "All":
        filtered_df = filtered_df[filtered_df["county"] == selected_county]

    if selected_stage != "All":
        filtered_df = filtered_df[filtered_df["project_stage"] == selected_stage]

    if selected_type != "All":
        filtered_df = filtered_df[filtered_df["project_type"] == selected_type]
else:
    filtered_df = projects_df

col1, col2, col3 = st.columns(3)
col1.metric("Qualified Opportunities", len(filtered_df))
col2.metric("Infrastructure Leads", len(leads_df))
col3.metric("Active User", user.get("role", "user"))

st.markdown("## Top Infrastructure Opportunities")

st.dataframe(
    filtered_df,
    use_container_width=True
)

st.markdown("## Infrastructure Leads")

st.dataframe(
    leads_df,
    use_container_width=True
)

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

if st.button("Sign Out"):
    st.session_state.user = None
    st.rerun()

st.caption("Allen Hammett AI • Infrastructure Intelligence System")
