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

# =====================================================
# FILTER KEYWORDS
# =====================================================

TARGET_KEYWORDS = [
    "data center",
    "datacenter",
    "substation",
    "switchyard",
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
]

# =====================================================
# DATABASE
# =====================================================

def run_query(sql: str, params=None) -> pd.DataFrame:

    conn = psycopg.connect(DATABASE_URL)

    try:
        return pd.read_sql(sql, conn, params=params)

    finally:
        conn.close()

# =====================================================
# AUTH
# =====================================================

def authenticate(email, password):

    sql = """
    select email, role
    from users
    where lower(email) = lower(%s)
      and password = %s
    limit 1
    """

    df = run_query(sql, (email.strip(), password))

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

    password = st.text_input(
        "Password",
        type="password"
    )

    if st.button("Login"):

        user = authenticate(email, password)

        if user:
            st.session_state.user = user
            st.rerun()

        else:
            st.error("Invalid credentials")

    st.stop()

# =====================================================
# PROJECTS
# =====================================================

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

    df["combined_text"] = (
        df.fillna("")
        .astype(str)
        .agg(" ".join, axis=1)
        .str.lower()
    )

    target_pattern = "|".join(TARGET_KEYWORDS)

    exclude_pattern = "|".join(EXCLUDED_KEYWORDS)

    df = df[
        df["combined_text"].str.contains(
            target_pattern,
            case=False,
            na=False
        )
        &
        ~df["combined_text"].str.contains(
            exclude_pattern,
            case=False,
            na=False
        )
    ].copy()

    df.drop(
        columns=["combined_text"],
        inplace=True,
        errors="ignore"
    )

    return df

# =====================================================
# LEADS
# =====================================================

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

# =====================================================
# LOGIN
# =====================================================

user = login_gate()

# =====================================================
# DATA
# =====================================================

projects_df = get_projects()

leads_df = get_leads()

filtered_df = projects_df.copy()

# =====================================================
# SIDEBAR
# =====================================================

st.sidebar.header("Infrastructure Intelligence Filters")

if not projects_df.empty:

    county_options = ["All"] + sorted(
        projects_df["county"]
        .dropna()
        .unique()
        .tolist()
    )

    state_options = ["All"] + sorted(
        projects_df["state"]
        .dropna()
        .unique()
        .tolist()
    )

    source_options = ["All"] + sorted(
        projects_df["source_name"]
        .dropna()
        .unique()
        .tolist()
    )

    stage_options = ["All"] + sorted(
        projects_df["project_stage"]
        .dropna()
        .unique()
        .tolist()
    )

    county_filter = st.sidebar.selectbox(
        "County",
        county_options
    )

    state_filter = st.sidebar.selectbox(
        "State",
        state_options
    )

    source_filter = st.sidebar.selectbox(
        "Source",
        source_options
    )

    stage_filter = st.sidebar.selectbox(
        "Stage",
        stage_options
    )

    if county_filter != "All":
        filtered_df = filtered_df[
            filtered_df["county"] == county_filter
        ]

    if state_filter != "All":
        filtered_df = filtered_df[
            filtered_df["state"] == state_filter
        ]

    if source_filter != "All":
        filtered_df = filtered_df[
            filtered_df["source_name"] == source_filter
        ]

    if stage_filter != "All":
        filtered_df = filtered_df[
            filtered_df["project_stage"] == stage_filter
        ]

# =====================================================
# HEADER
# =====================================================

st.title("Infrastructure Intelligence Platform")

st.markdown(
    "Allen Hammett AI — Private Infrastructure / Data Center Intelligence"
)

# =====================================================
# METRICS
# =====================================================

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Qualified Opportunities",
    len(filtered_df)
)

col2.metric(
    "Infrastructure Leads",
    len(leads_df)
)

col3.metric(
    "Counties",
    filtered_df["county"].nunique()
)

col4.metric(
    "Sources",
    filtered_df["source_name"].nunique()
)

# =====================================================
# PROJECTS TABLE
# =====================================================

st.markdown("## Top Infrastructure Opportunities")

st.dataframe(
    filtered_df,
    use_container_width=True
)

# =====================================================
# LEADS TABLE
# =====================================================

st.markdown("## Infrastructure Leads")

st.dataframe(
    leads_df,
    use_container_width=True
)

# =====================================================
# EXPORT
# =====================================================

st.markdown("## Export Leads")

if not leads_df.empty:

    csv = leads_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download Leads CSV",
        data=csv,
        file_name="infrastructure_leads.csv",
        mime="text/csv"
    )

# =====================================================
# SIGN OUT
# =====================================================

if st.button("Sign Out"):

    st.session_state.user = None

    st.rerun()

# =====================================================
# FOOTER
# =====================================================

st.caption(
    "Allen Hammett AI • Infrastructure Intelligence System"
)
