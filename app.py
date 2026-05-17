import os
import pandas as pd
import streamlit as st
import psycopg2
import pydeck as pdk
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Allen Hammett AI", layout="wide")

DATABASE_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))


def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg2.connect(DATABASE_URL)
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

    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

    if submitted:
        user = authenticate(email, password)
        if user:
            st.session_state.user = user
            st.rerun()
        else:
            st.error("Invalid credentials")

    st.stop()


def get_projects(time_horizon):
    interval_map = {
        "30 Days": "30 days",
        "90 Days": "90 days",
        "12 Months": "12 months",
        "24 Months": "24 months",
    }

    where_clause = ""
    if time_horizon in interval_map:
        where_clause = f"where created_at >= now() - interval '{interval_map[time_horizon]}'"

    sql = f"""
    select
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
        created_at
    from projects
    {where_clause}
    order by early_capture_score desc nulls last, created_at desc
    limit 5000
    """

    return run_query(sql)


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


user = login_gate()

st.sidebar.header("Executive Filters")

time_horizon = st.sidebar.selectbox(
    "Intelligence Timeline",
    ["30 Days", "90 Days", "12 Months", "24 Months", "All Intelligence"],
    index=4,
)

projects_df = get_projects(time_horizon)
leads_df = get_leads()

if not projects_df.empty:
    projects_df["early_capture_score"] = projects_df["early_capture_score"].fillna(0).astype(int)
    projects_df["capture_stage"] = projects_df["early_capture_score"].apply(capture_stage)
    projects_df["map_color"] = projects_df["early_capture_score"].apply(map_color)
    projects_df["map_radius"] = projects_df["early_capture_score"].apply(map_radius)

filtered_df = projects_df.copy()

search_term = st.sidebar.text_input("Search keyword")
predictive_only = st.sidebar.checkbox("Predictive signals only")
prime_only = st.sidebar.checkbox("Prime positioning only")

if not projects_df.empty:
    county_options = ["All"] + sorted(projects_df["county"].dropna().unique().tolist())
    category_options = ["All"] + sorted(projects_df["intelligence_category"].dropna().unique().tolist())
    capture_options = ["All"] + sorted(projects_df["capture_stage"].dropna().unique().tolist())
    corridor_options = ["All"] + sorted(projects_df["corridor_region"].dropna().unique().tolist())
    stage_options = ["All"] + sorted(projects_df["project_stage"].dropna().unique().tolist())

    county_filter = st.sidebar.selectbox("County", county_options)
    category_filter = st.sidebar.selectbox("Intelligence Category", category_options)
    capture_filter = st.sidebar.selectbox("Capture Stage", capture_options)
    corridor_filter = st.sidebar.selectbox("Corridor Region", corridor_options)
    stage_filter = st.sidebar.selectbox("Project Stage", stage_options)

    if county_filter != "All":
        filtered_df = filtered_df[filtered_df["county"] == county_filter]

    if category_filter != "All":
        filtered_df = filtered_df[filtered_df["intelligence_category"] == category_filter]

    if capture_filter != "All":
        filtered_df = filtered_df[filtered_df["capture_stage"] == capture_filter]

    if corridor_filter != "All":
        filtered_df = filtered_df[filtered_df["corridor_region"] == corridor_filter]

    if stage_filter != "All":
        filtered_df = filtered_df[filtered_df["project_stage"] == stage_filter]

    if predictive_only:
        filtered_df = filtered_df[filtered_df["predictive_signal"] == True]

    if prime_only:
        filtered_df = filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]

    if search_term:
        mask = filtered_df.astype(str).apply(
            lambda row: row.str.contains(search_term, case=False, na=False).any(),
            axis=1,
        )
        filtered_df = filtered_df[mask]


st.title("Infrastructure Intelligence Platform")
st.markdown("Allen Hammett AI — Executive Infrastructure / Early Capture Intelligence")

st.markdown("### Capture Intelligence Legend")

legend_cols = st.columns(5)
legend_cols[0].success("Prime Positioning 90–100")
legend_cols[1].info("Strategic Development 75–89")
legend_cols[2].markdown("**Active Monitoring**  \n50–74")
legend_cols[3].markdown("**Early Identification**  \n25–49")
legend_cols[4].markdown("**Historical Context**  \n0–24")

col1, col2, col3, col4, col5 = st.columns(5)

mapped_df = filtered_df.copy()

if not mapped_df.empty:
    mapped_df["latitude"] = pd.to_numeric(mapped_df["latitude"], errors="coerce")
    mapped_df["longitude"] = pd.to_numeric(mapped_df["longitude"], errors="coerce")
    mapped_df = mapped_df.dropna(subset=["latitude", "longitude"])
    mapped_df = mapped_df[
        (mapped_df["latitude"].between(36, 40.5))
        & (mapped_df["longitude"].between(-84, -75))
    ]

col1.metric("Qualified Signals", len(filtered_df))
col2.metric("Prime Positioning", len(filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]) if not filtered_df.empty else 0)
col3.metric("Predictive Signals", len(filtered_df[filtered_df["predictive_signal"] == True]) if not filtered_df.empty else 0)
col4.metric("Mapped Records", len(mapped_df))
col5.metric("Leads", len(leads_df))

st.markdown("## Infrastructure Intelligence Map")

if not mapped_df.empty:
    map_df = mapped_df.rename(columns={"latitude": "lat", "longitude": "lon"}).copy()

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
                <b>Corridor:</b> {corridor_region}<br/>
                <b>County:</b> {county}<br/>
                <b>Stage:</b> {project_stage}
                """,
                "style": {"backgroundColor": "black", "color": "white"},
            },
        )
    )

    with st.expander("Mapped Records Preview"):
        st.dataframe(
            map_df[
                [
                    "canonical_project_name",
                    "capture_stage",
                    "early_capture_score",
                    "county",
                    "lat",
                    "lon",
                ]
            ],
            use_container_width=True,
        )

else:
    st.warning("No mapped records found for the current filters.")

st.markdown("## Executive Priority Intelligence")

display_cols = [
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
    "county",
    "state",
    "source_name",
    "predictive_signal",
    "utility_related",
    "hyperscale_related",
    "transmission_related",
    "fiber_related",
    "latitude",
    "longitude",
    "created_at",
]

existing_cols = [c for c in display_cols if c in filtered_df.columns]
st.dataframe(filtered_df[existing_cols], use_container_width=True)

st.markdown("## Infrastructure Leads")
st.dataframe(leads_df, use_container_width=True)

st.markdown("## Export Intelligence")

if not filtered_df.empty:
    opportunities_csv = filtered_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download Opportunities CSV",
        data=opportunities_csv,
        file_name="executive_infrastructure_opportunities.csv",
        mime="text/csv",
    )

if not leads_df.empty:
    leads_csv = leads_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download Leads CSV",
        data=leads_csv,
        file_name="infrastructure_leads.csv",
        mime="text/csv",
    )

if st.button("Sign Out"):
    st.session_state.user = None
    st.rerun()

st.caption("Allen Hammett AI • Infrastructure Early Capture Intelligence System")
