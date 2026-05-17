import os
import pandas as pd
import streamlit as st
import psycopg2
import pydeck as pdk
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Allen Hammett AI",
    layout="wide"
)

DATABASE_URL = st.secrets.get(
    "DATABASE_URL",
    os.getenv("DATABASE_URL")
)


# ---------------------------------------------------
# DATABASE
# ---------------------------------------------------

def run_query(sql: str, params=None):

    conn = psycopg2.connect(DATABASE_URL)

    try:
        return pd.read_sql(sql, conn, params=params)

    finally:
        conn.close()


# ---------------------------------------------------
# AUTH
# ---------------------------------------------------

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

    st.subheader(
        "Secure Infrastructure Intelligence Access"
    )

    with st.form("login_form"):

        email = st.text_input("Email")

        password = st.text_input(
            "Password",
            type="password"
        )

        submitted = st.form_submit_button("Login")

    if submitted:

        user = authenticate(email, password)

        if user:

            st.session_state.user = user
            st.rerun()

        else:
            st.error("Invalid credentials")

    st.stop()


# ---------------------------------------------------
# PROJECTS
# ---------------------------------------------------

def get_projects(time_horizon):

    interval_map = {
        "30 Days": "30 days",
        "90 Days": "90 days",
        "12 Months": "12 months",
        "24 Months": "24 months",
    }

    where_clause = ""

    if time_horizon in interval_map:

        where_clause = f"""
        where created_at >= now() - interval '{interval_map[time_horizon]}'
        """

    sql = f"""

    select
        id,
        case_number,
        canonical_project_name,
        project_stage,
        project_type,
        county,
        state,
        source_name,
        source_type,
        intelligence_category,
        infrastructure_type,
        strategic_priority,
        corridor_region,
        market_cluster,
        early_capture_score,
        predictive_signal,
        utility_related,
        hyperscale_related,
        transmission_related,
        fiber_related,
        latitude,
        longitude,
        created_at,

        applicant_name,
        permit_description,
        source_url,
        source_document_url,
        filing_date,
        utility_provider,
        raw_text

    from projects

    {where_clause}

    order by
        early_capture_score desc nulls last,
        created_at desc

    limit 5000

    """

    return run_query(sql)


# ---------------------------------------------------
# LEADS
# ---------------------------------------------------

def get_leads():

    return run_query(
        """

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
    )


# ---------------------------------------------------
# SCORING
# ---------------------------------------------------

def capture_stage(score):

    if score >= 90:
        return "Prime Positioning"

    if score >= 75:
        return "Strategic Development"

    if score >= 50:
        return "Active Monitoring"

    if score >= 25:
        return "Early Identification"

    return "Historical Context"


def map_color(score):

    if score >= 90:
        return [0, 180, 120, 220]

    if score >= 75:
        return [20, 80, 180, 220]

    if score >= 50:
        return [0, 180, 200, 220]

    if score >= 25:
        return [100, 120, 140, 200]

    return [55, 60, 70, 170]


def map_radius(score):

    if score >= 90:
        return 1800

    if score >= 75:
        return 1400

    if score >= 50:
        return 1100

    if score >= 25:
        return 850

    return 650


# ---------------------------------------------------
# APP START
# ---------------------------------------------------

user = login_gate()

st.sidebar.header("Executive Filters")

time_horizon = st.sidebar.selectbox(
    "Intelligence Timeline",
    [
        "30 Days",
        "90 Days",
        "12 Months",
        "24 Months",
        "All Intelligence"
    ],
    index=4,
)

projects_df = get_projects(time_horizon)

leads_df = get_leads()

if not projects_df.empty:

    projects_df["early_capture_score"] = (
        projects_df["early_capture_score"]
        .fillna(0)
        .astype(int)
    )

    projects_df["capture_stage"] = (
        projects_df["early_capture_score"]
        .apply(capture_stage)
    )

    projects_df["map_color"] = (
        projects_df["early_capture_score"]
        .apply(map_color)
    )

    projects_df["map_radius"] = (
        projects_df["early_capture_score"]
        .apply(map_radius)
    )

filtered_df = projects_df.copy()


# ---------------------------------------------------
# FILTERS
# ---------------------------------------------------

search_term = st.sidebar.text_input(
    "Search keyword"
)

predictive_only = st.sidebar.checkbox(
    "Predictive signals only"
)

prime_only = st.sidebar.checkbox(
    "Prime positioning only"
)

if not projects_df.empty:

    county_options = ["All"] + sorted(
        projects_df["county"]
        .dropna()
        .unique()
        .tolist()
    )

    category_options = ["All"] + sorted(
        projects_df["intelligence_category"]
        .dropna()
        .unique()
        .tolist()
    )

    capture_options = ["All"] + sorted(
        projects_df["capture_stage"]
        .dropna()
        .unique()
        .tolist()
    )

    county_filter = st.sidebar.selectbox(
        "County",
        county_options
    )

    category_filter = st.sidebar.selectbox(
        "Intelligence Category",
        category_options
    )

    capture_filter = st.sidebar.selectbox(
        "Capture Stage",
        capture_options
    )

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

    if predictive_only:

        filtered_df = filtered_df[
            filtered_df["predictive_signal"] == True
        ]

    if prime_only:

        filtered_df = filtered_df[
            filtered_df["capture_stage"] == "Prime Positioning"
        ]

    if search_term:

        mask = filtered_df.astype(str).apply(
            lambda row: row.str.contains(
                search_term,
                case=False,
                na=False
            ).any(),
            axis=1,
        )

        filtered_df = filtered_df[mask]


# ---------------------------------------------------
# HEADER
# ---------------------------------------------------

st.title(
    "Infrastructure Intelligence Platform"
)

st.markdown(
    "Allen Hammett AI — Executive Infrastructure / Early Capture Intelligence"
)


# ---------------------------------------------------
# LEGEND
# ---------------------------------------------------

st.markdown(
    "### Capture Intelligence Legend"
)

legend_cols = st.columns(5)

legend_cols[0].success(
    "Prime Positioning 90–100"
)

legend_cols[1].info(
    "Strategic Development 75–89"
)

legend_cols[2].markdown(
    "**Active Monitoring**  \n50–74"
)

legend_cols[3].markdown(
    "**Early Identification**  \n25–49"
)

legend_cols[4].markdown(
    "**Historical Context**  \n0–24"
)


# ---------------------------------------------------
# METRICS
# ---------------------------------------------------

mapped_df = filtered_df.copy()

if not mapped_df.empty:

    mapped_df["latitude"] = pd.to_numeric(
        mapped_df["latitude"],
        errors="coerce"
    )

    mapped_df["longitude"] = pd.to_numeric(
        mapped_df["longitude"],
        errors="coerce"
    )

    mapped_df = mapped_df.dropna(
        subset=["latitude", "longitude"]
    )

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric(
    "Qualified Signals",
    len(filtered_df)
)

col2.metric(
    "Prime Positioning",
    len(
        filtered_df[
            filtered_df["capture_stage"]
            == "Prime Positioning"
        ]
    )
)

col3.metric(
    "Predictive Signals",
    len(
        filtered_df[
            filtered_df["predictive_signal"]
            == True
        ]
    )
)

col4.metric(
    "Mapped Records",
    len(mapped_df)
)

col5.metric(
    "Leads",
    len(leads_df)
)


# ---------------------------------------------------
# MAP
# ---------------------------------------------------

st.markdown(
    "## Infrastructure Intelligence Map"
)

if not mapped_df.empty:

    map_df = mapped_df.rename(
        columns={
            "latitude": "lat",
            "longitude": "lon"
        }
    )

    center_lat = float(map_df["lat"].median())

    center_lon = float(map_df["lon"].median())

    st.pydeck_chart(

        pdk.Deck(

            map_style=pdk.map_styles.CARTO_DARK,

            initial_view_state=pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=9,
                pitch=35,
            ),

            layers=[

                pdk.Layer(
                    "ScatterplotLayer",

                    data=map_df,

                    get_position="[lon, lat]",

                    get_color="map_color",

                    get_radius="map_radius",

                    pickable=True,

                    auto_highlight=True,
                )

            ],

            tooltip={
                "html": """

                <b>Project:</b> {canonical_project_name}<br/>

                <b>Capture Stage:</b> {capture_stage}<br/>

                <b>Score:</b> {early_capture_score}<br/>

                <b>Category:</b> {intelligence_category}<br/>

                <b>County:</b> {county}<br/>

                <b>Stage:</b> {project_stage}

                """,

                "style": {
                    "backgroundColor": "black",
                    "color": "white"
                },
            },
        )
    )

else:

    st.warning(
        "No mapped records found."
    )


# ---------------------------------------------------
# PROJECT DETAILS
# ---------------------------------------------------

st.markdown(
    "## Executive Intelligence Profiles"
)

if not filtered_df.empty:

    for _, row in filtered_df.head(50).iterrows():

        with st.expander(
            f"{row['canonical_project_name']} "
            f"({row['capture_stage']})"
        ):

            col1, col2 = st.columns(2)

            with col1:

                st.markdown(
                    f"**Case Number:** {row.get('case_number')}"
                )

                st.markdown(
                    f"**County:** {row.get('county')}"
                )

                st.markdown(
                    f"**Project Stage:** {row.get('project_stage')}"
                )

                st.markdown(
                    f"**Infrastructure Type:** {row.get('infrastructure_type')}"
                )

                st.markdown(
                    f"**Intelligence Category:** {row.get('intelligence_category')}"
                )

                st.markdown(
                    f"**Capture Score:** {row.get('early_capture_score')}"
                )

                st.markdown(
                    f"**Applicant:** {row.get('applicant_name')}"
                )

                st.markdown(
                    f"**Utility Provider:** {row.get('utility_provider')}"
                )

            with col2:

                st.markdown(
                    f"**Corridor Region:** {row.get('corridor_region')}"
                )

                st.markdown(
                    f"**Market Cluster:** {row.get('market_cluster')}"
                )

                st.markdown(
                    f"**Filing Date:** {row.get('filing_date')}"
                )

                st.markdown(
                    f"**Source Name:** {row.get('source_name')}"
                )

                st.markdown(
                    f"**Source Type:** {row.get('source_type')}"
                )

                st.markdown(
                    f"**Predictive Signal:** {row.get('predictive_signal')}"
                )

                st.markdown(
                    f"**Created At:** {row.get('created_at')}"
                )

            st.markdown("### Permit Description")

            st.write(
                row.get("permit_description")
            )

            st.markdown("### Raw Filing Intelligence")

            st.code(
                str(row.get("raw_text"))
            )

            if row.get("source_url"):

                st.markdown(
                    f"[Open Source Record]({row.get('source_url')})"
                )

            if row.get("source_document_url"):

                st.markdown(
                    f"[Open Filing Document]({row.get('source_document_url')})"
                )


# ---------------------------------------------------
# LEADS
# ---------------------------------------------------

st.markdown(
    "## Infrastructure Leads"
)

st.dataframe(
    leads_df,
    use_container_width=True
)


# ---------------------------------------------------
# EXPORTS
# ---------------------------------------------------

st.markdown(
    "## Export Intelligence"
)

if not filtered_df.empty:

    opportunities_csv = (
        filtered_df
        .to_csv(index=False)
        .encode("utf-8")
    )

    st.download_button(
        label="Download Opportunities CSV",
        data=opportunities_csv,
        file_name="executive_infrastructure_opportunities.csv",
        mime="text/csv",
    )


# ---------------------------------------------------
# SIGN OUT
# ---------------------------------------------------

if st.button("Sign Out"):

    st.session_state.user = None

    st.rerun()


st.caption(
    "Allen Hammett AI • Infrastructure Intelligence System"
)
