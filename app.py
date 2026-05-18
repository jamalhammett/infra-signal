import os
import streamlit as st
import pandas as pd
import psycopg2
import pydeck as pdk
from sqlalchemy import create_engine
from dotenv import load_dotenv

# =========================================================
# LOAD ENVIRONMENT
# =========================================================

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# =========================================================
# PAGE CONFIG
# =========================================================

st.set_page_config(
    page_title="Allen Hammett AI",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# DATABASE CONNECTION
# =========================================================

engine = create_engine(DATABASE_URL)

# =========================================================
# AUTHENTICATION
# =========================================================

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

VALID_USERS = {
    "jamal.hammett@allenhammett.com": "AllenHammett2025!"
}

if not st.session_state.authenticated:

    st.title("Allen Hammett AI")
    st.subheader("Secure Infrastructure Intelligence Access")

    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")

        submitted = st.form_submit_button("Login")

        if submitted:
            if email in VALID_USERS and VALID_USERS[email] == password:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Invalid credentials")

    st.stop()

# =========================================================
# DATA ACCESS
# =========================================================

@st.cache_data(ttl=3600)
def run_query(query):
    return pd.read_sql(query, engine)

@st.cache_data(ttl=3600)
def get_projects():

    query = """
    select *
    from projects
    order by early_capture_score desc nulls last
    limit 5000
    """

    return run_query(query)

@st.cache_data(ttl=3600)
def get_leads():

    query = """
    select *
    from infrastructure_leads
    order by created_at desc
    limit 500
    """

    return run_query(query)

projects_df = get_projects()
leads_df = get_leads()

# =========================================================
# SIDEBAR FILTERS
# =========================================================

st.sidebar.title("Executive Filters")

timeline_filter = st.sidebar.selectbox(
    "Intelligence Timeline",
    ["All Intelligence", "Recent 30 Days", "Recent 90 Days"]
)

search_keyword = st.sidebar.text_input("Search keyword")

predictive_only = st.sidebar.checkbox("Predictive signals only")
prime_only = st.sidebar.checkbox("Prime positioning only")

county_options = ["All"] + sorted(
    projects_df["county"].dropna().unique().tolist()
)

county_filter = st.sidebar.selectbox(
    "County",
    county_options
)

category_options = ["All"] + sorted(
    projects_df["intelligence_category"].dropna().unique().tolist()
)

category_filter = st.sidebar.selectbox(
    "Intelligence Category",
    category_options
)

capture_options = ["All"] + sorted(
    projects_df["capture_stage"].dropna().unique().tolist()
)

capture_filter = st.sidebar.selectbox(
    "Capture Stage",
    capture_options
)

corridor_options = ["All"] + sorted(
    projects_df["corridor_region"].dropna().unique().tolist()
)

corridor_filter = st.sidebar.selectbox(
    "Corridor Region",
    corridor_options
)

project_stage_options = ["All"] + sorted(
    projects_df["project_stage"].dropna().unique().tolist()
)

project_stage_filter = st.sidebar.selectbox(
    "Project Stage",
    project_stage_options
)

# =========================================================
# FILTER LOGIC
# =========================================================

filtered_df = projects_df.copy()

if search_keyword:
    filtered_df = filtered_df[
        filtered_df["canonical_project_name"]
        .astype(str)
        .str.contains(search_keyword, case=False, na=False)
    ]

if predictive_only:
    filtered_df = filtered_df[
        filtered_df["predictive_signal"] == True
    ]

if prime_only:
    filtered_df = filtered_df[
        filtered_df["capture_stage"] == "Prime Positioning"
    ]

if county_filter != "All":
    filtered_df = filtered_df[
        filtered_df["county"] == county_filter
    ]

if category_filter != "All":
    filtered_df = filtered_df[
        filtered_df["intelligence_category"] == category_filter
    ]

if capture_filter != "All":
    filtered_df = filtered_df[
        filtered_df["capture_stage"] == capture_filter
    ]

if corridor_filter != "All":
    filtered_df = filtered_df[
        filtered_df["corridor_region"] == corridor_filter
    ]

if project_stage_filter != "All":
    filtered_df = filtered_df[
        filtered_df["project_stage"] == project_stage_filter
    ]

# =========================================================
# HEADER
# =========================================================

st.title("Infrastructure Intelligence Platform")

st.markdown(
    "Allen Hammett AI — Executive Infrastructure / Early Capture Intelligence"
)

# =========================================================
# LEGEND
# =========================================================

st.header("Capture Intelligence Legend")

legend_cols = st.columns(5)

with legend_cols[0]:
    st.success("Prime Positioning 90–100")

with legend_cols[1]:
    st.info("Strategic Development 75–89")

with legend_cols[2]:
    st.warning("Active Monitoring 50–74")

with legend_cols[3]:
    st.warning("Early Identification 25–49")

with legend_cols[4]:
    st.error("Historical Context 0–24")

# =========================================================
# KPI ROW
# =========================================================

kpi_cols = st.columns(5)

with kpi_cols[0]:
    st.metric("Qualified Signals", len(projects_df))

with kpi_cols[1]:
    st.metric(
        "Prime Positioning",
        len(
            filtered_df[
                filtered_df["capture_stage"] == "Prime Positioning"
            ]
        )
    )

with kpi_cols[2]:
    st.metric(
        "Predictive Signals",
        len(
            filtered_df[
                filtered_df["predictive_signal"] == True
            ]
        )
    )

with kpi_cols[3]:
    st.metric(
        "Mapped Records",
        len(
            filtered_df[
                filtered_df["latitude"].notna()
            ]
        )
    )

with kpi_cols[4]:
    st.metric("Leads", len(leads_df))

# =========================================================
# MAP
# =========================================================

st.header("Infrastructure Intelligence Map")

map_df = filtered_df[
    filtered_df["latitude"].notna()
].copy()

if not map_df.empty:

    map_df["color"] = map_df["capture_stage"].map({
        "Prime Positioning": [0, 200, 83],
        "Strategic Development": [0, 102, 255],
        "Active Monitoring": [255, 193, 7],
        "Early Identification": [255, 87, 34],
        "Historical Context": [158, 158, 158]
    })

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position='[longitude, latitude]',
        get_radius=1200,
        get_fill_color="color",
        pickable=True,
        auto_highlight=True
    )

    view_state = pdk.ViewState(
        latitude=39.0438,
        longitude=-77.4874,
        zoom=8,
        pitch=40
    )

    tooltip = {
        "html": """
        <b>Project:</b> {canonical_project_name}<br/>
        <b>Capture Stage:</b> {capture_stage}<br/>
        <b>Score:</b> {early_capture_score}<br/>
        <b>Category:</b> {intelligence_category}<br/>
        <b>Corridor:</b> {corridor_region}<br/>
        <b>County:</b> {county}<br/>
        <b>Stage:</b> {project_stage}
        """,
        "style": {
            "backgroundColor": "black",
            "color": "white"
        }
    }

    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style="mapbox://styles/mapbox/dark-v10"
        )
    )

else:
    st.warning("No geospatial infrastructure records found.")

# =========================================================
# EXECUTIVE INTELLIGENCE PROFILES
# =========================================================

st.header("Executive Intelligence Profiles")

for _, row in filtered_df.head(100).iterrows():

    capture_stage = row.get("capture_stage", "Unknown")

    with st.expander(
        f"{row['canonical_project_name']} ({capture_stage})"
    ):

        col1, col2 = st.columns(2)

        with col1:

            st.markdown(f"**Case Number:** {row.get('case_number', 'N/A')}")
            st.markdown(f"**County:** {row.get('county', 'N/A')}")
            st.markdown(f"**Project Stage:** {row.get('project_stage', 'N/A')}")
            st.markdown(f"**Infrastructure Type:** {row.get('infrastructure_type', 'N/A')}")
            st.markdown(f"**Intelligence Category:** {row.get('intelligence_category', 'N/A')}")
            st.markdown(f"**Capture Score:** {row.get('early_capture_score', 'N/A')}")

            st.markdown(f"**Applicant:** {row.get('applicant', 'Unknown')}")
            st.markdown(f"**Utility Provider:** {row.get('utility_dependency', 'Unknown')}")

        with col2:

            st.markdown(f"**Corridor Region:** {row.get('corridor_region', 'N/A')}")
            st.markdown(f"**Market Cluster:** {row.get('market_cluster', 'N/A')}")
            st.markdown(f"**Filing Date:** {row.get('filing_date', 'N/A')}")
            st.markdown(f"**Source Name:** {row.get('source_name', 'N/A')}")
            st.markdown(f"**Source Type:** {row.get('source_type', 'N/A')}")
            st.markdown(f"**Predictive Signal:** {row.get('predictive_signal', False)}")
            st.markdown(f"**Estimated MW Demand:** {row.get('estimated_power_mw', 'Unknown')}")
            st.markdown(f"**Created At:** {row.get('created_at', 'N/A')}")

        st.markdown("---")

        st.subheader("Executive Strategic Assessment")

        strategic_notes = row.get("strategic_notes")

        if strategic_notes:
            st.info(strategic_notes)
        else:
            st.warning("No strategic assessment generated yet.")

        st.subheader("Infrastructure Risk Flags")

        risk_flags = row.get("risk_flags")

        if risk_flags:
            for flag in str(risk_flags).split(","):
                st.error(flag.strip())
        else:
            st.success("No major infrastructure risks currently detected.")

        st.subheader("Permit Description")

        permit_description = row.get("permit_description")

        if permit_description:
            st.write(permit_description)
        else:
            st.warning("No permit description stored yet.")

        st.subheader("Raw Filing Intelligence")

        raw_text = row.get("raw_text")

        if raw_text:
            st.code(raw_text[:5000])
        else:
            st.warning("No raw filing text stored yet.")

# =========================================================
# EXECUTIVE PRIORITY TABLE
# =========================================================

st.header("Executive Priority Intelligence")

priority_columns = [
    "capture_stage",
    "early_capture_score",
    "intelligence_category",
    "infrastructure_type",
    "corridor_region",
    "market_cluster",
    "case_number",
    "canonical_project_name",
    "project_stage",
    "project_type",
    "county"
]

available_priority_columns = [
    c for c in priority_columns if c in filtered_df.columns
]

st.dataframe(
    filtered_df[available_priority_columns],
    use_container_width=True
)

# =========================================================
# LEADS
# =========================================================

st.header("Infrastructure Leads")

st.dataframe(
    leads_df,
    use_container_width=True
)

# =========================================================
# EXPORTS
# =========================================================

st.header("Export Intelligence")

csv_projects = filtered_df.to_csv(index=False)

st.download_button(
    "Download Opportunities CSV",
    csv_projects,
    "infrastructure_opportunities.csv",
    "text/csv"
)

csv_leads = leads_df.to_csv(index=False)

st.download_button(
    "Download Leads CSV",
    csv_leads,
    "infrastructure_leads.csv",
    "text/csv"
)

# =========================================================
# SIGN OUT
# =========================================================

if st.button("Sign Out"):
    st.session_state.authenticated = False
    st.rerun()

st.caption("Allen Hammett AI • Infrastructure Intelligence System")
