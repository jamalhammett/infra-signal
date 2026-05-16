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
    "data center",
    "datacenter",
    "transmission",
    "utility",
    "power",
    "energy",
    "fiber",
    "telecom",
    "hyperscale",
    "cloud",
    "server",
    "industrial",
]

SUBSTATION_ALLOWED_CONTEXT = [
    "novec",
    "dominion",
    "data center",
    "datacenter",
    "transmission",
    "energy",
    "power",
    "utility",
]

EXCLUDED_KEYWORDS = [
    "sidewalk",
    "trail",
    "path",
    "driveway",
    "subdivision",
    "townhome",
    "townhomes",
    "single family",
    "residential",
    "lot ",
    "lots",
    "farm",
    "vineyard",
    "conservancy",
    "school",
    "church",
    "playground",
    "park",
    "landscape",
    "forest",
    "stormwater",
    "road widening",
    "firehouse",
    "barrister",
    "golden",
    "auto world",
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


def is_qualified_project(row):
    text = " ".join([str(v) for v in row.fillna("").values]).lower()

    if any(bad in text for bad in EXCLUDED_KEYWORDS):
        return False

    if "substation" in text:
        return any(ctx in text for ctx in SUBSTATION_ALLOWED_CONTEXT)

    return any(good in text for good in TARGET_KEYWORDS)


def get_projects():
    sql = """
    select
        case_number,
        canonical_project_name,
        project_stage,
        project_type,
        county,
        state,
        source_name,
        source_type,
        created_at
    from projects
    where created_at >= now() - interval '90 days'
    order by created_at desc
    limit 2000
    """

    df = run_query(sql)

    if df.empty:
        return df

    df = df[df.apply(is_qualified_project, axis=1)].copy()

    return df


def get_leads():
    sql = """
    select
        company,
        contact_name,
        title,
        email,
        phone,
        county,
        state,
        source_name,
        created_at
    from leads
    order by created_at desc
    limit 250
    """

    return run_query(sql)


user = login_gate()

projects_df = get_projects()
leads_df = get_leads()
filtered_df = projects_df.copy()

st.sidebar.header("Infrastructure Intelligence Filters")

if not projects_df.empty:
    county_options = ["All"] + sorted(projects_df["county"].dropna().unique().tolist())
    state_options = ["All"] + sorted(projects_df["state"].dropna().unique().tolist())
    source_options = ["All"] + sorted(projects_df["source_name"].dropna().unique().tolist())
    stage_options = ["All"] + sorted(projects_df["project_stage"].dropna().unique().tolist())

    county_filter = st.sidebar.selectbox("County", county_options)
    state_filter = st.sidebar.selectbox("State", state_options)
    source_filter = st.sidebar.selectbox("Source", source_options)
    stage_filter = st.sidebar.selectbox("Stage", stage_options)

    if county_filter != "All":
        filtered_df = filtered_df[filtered_df["county"] == county_filter]

    if state_filter != "All":
        filtered_df = filtered_df[filtered_df["state"] == state_filter]

    if source_filter != "All":
        filtered_df = filtered_df[filtered_df["source_name"] == source_filter]

    if stage_filter != "All":
        filtered_df = filtered_df[filtered_df["project_stage"] == stage_filter]


st.title("Infrastructure Intelligence Platform")
st.markdown("Allen Hammett AI — Private Infrastructure / Data Center Intelligence")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Qualified Opportunities", len(filtered_df))
col2.metric("Infrastructure Leads", len(leads_df))
col3.metric("Counties", filtered_df["county"].nunique() if not filtered_df.empty else 0)
col4.metric("Sources", filtered_df["source_name"].nunique() if not filtered_df.empty else 0)

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

if st.button("Sign Out"):
    st.session_state.user = None
    st.rerun()

st.caption("Allen Hammett AI • Infrastructure Intelligence System")
