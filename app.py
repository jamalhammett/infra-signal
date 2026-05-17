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


def get_projects():
    return run_query(
        """
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
        where created_at >= now() - interval '90 days'
        order by early_capture_score desc nulls last, created_at desc
        limit 2000
        """
    )


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


user = login_gate()

projects_df = get_projects()
leads_df = get_leads()
filtered_df = projects_df.copy()

st.sidebar.header("Executive Filters")

search_term = st.sidebar.text_input("Search keyword")
predictive_only = st.sidebar.checkbox("Predictive signals only")
high_priority_only = st.sidebar.checkbox("High priority only")

if not projects_df.empty:
    county_options = ["All"] + sorted(projects_df["county"].dropna().unique().tolist())
    state_options = ["All"] + sorted(projects_df["state"].dropna().unique().tolist())
    category_options = ["All"] + sorted(projects_df["intelligence_category"].dropna().unique().tolist())
    priority_options = ["All"] + sorted(projects_df["strategic_priority"].dropna().unique().tolist())
    corridor_options = ["All"] + sorted(projects_df["corridor_region"].dropna().unique().tolist())
    stage_options = ["All"] + sorted(projects_df["project_stage"].dropna().unique().tolist())

    county_filter = st.sidebar.selectbox("County", county_options)
    state_filter = st.sidebar.selectbox("State", state_options)
    category_filter = st.sidebar.selectbox("Intelligence Category", category_options)
    priority_filter = st.sidebar.selectbox("Strategic Priority", priority_options)
    corridor_filter = st.sidebar.selectbox("Corridor Region", corridor_options)
    stage_filter = st.sidebar.selectbox("Stage", stage_options)

    if county_filter != "All":
        filtered_df = filtered_df[filtered_df["county"] == county_filter]

    if state_filter != "All":
        filtered_df = filtered_df[filtered_df["state"] == state_filter]

    if category_filter != "All":
        filtered_df = filtered_df[filtered_df["intelligence_category"] == category_filter]

    if priority_filter != "All":
        filtered_df = filtered_df[filtered_df["strategic_priority"] == priority_filter]

    if corridor_filter != "All":
        filtered_df = filtered_df[filtered_df["corridor_region"] == corridor_filter]

    if stage_filter != "All":
        filtered_df = filtered_df[filtered_df["project_stage"] == stage_filter]

    if predictive_only:
        filtered_df = filtered_df[filtered_df["predictive_signal"] == True]

    if high_priority_only:
        filtered_df = filtered_df[filtered_df["strategic_priority"] == "HIGH"]

    if search_term:
        mask = filtered_df.astype(str).apply(
            lambda row: row.str.contains(search_term, case=False, na=False).any(),
            axis=1,
        )
        filtered_df = filtered_df[mask]


st.title("Infrastructure Intelligence Platform")
st.markdown("Allen Hammett AI — Executive Infrastructure / Early Capture Intelligence")

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Qualified Signals", len(filtered_df))
col2.metric("High Priority", len(filtered_df[filtered_df["strategic_priority"] == "HIGH"]) if not filtered_df.empty else 0)
col3.metric("Predictive Signals", len(filtered_df[filtered_df["predictive_signal"] == True]) if not filtered_df.empty else 0)
col4.metric("Mapped Records", len(filtered_df.dropna(subset=["latitude", "longitude"])) if not filtered_df.empty else 0)
col5.metric("Leads", len(leads_df))

st.markdown("## Infrastructure Intelligence Map")

map_df = filtered_df.dropna(subset=["latitude", "longitude"]).copy()

if not map_df.empty:
    map_df = map_df.rename(columns={"latitude": "lat", "longitude": "lon"})

    st.pydeck_chart(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/dark-v10",
            initial_view_state=pdk.ViewState(
                latitude=39.0438,
                longitude=-77.4874,
                zoom=8,
                pitch=40,
            ),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=map_df,
                    get_position="[lon, lat]",
                    get_color="[0, 255, 140, 190]",
                    get_radius=1000,
                    pickable=True,
                    auto_highlight=True,
                )
            ],
            tooltip={
                "html": """
                <b>Project:</b> {canonical_project_name}<br/>
                <b>Priority:</b> {strategic_priority}<br/>
                <b>Score:</b> {early_capture_score}<br/>
                <b>Category:</b> {intelligence_category}<br/>
                <b>County:</b> {county}<br/>
                <b>Stage:</b> {project_stage}
                """,
                "style": {"backgroundColor": "black", "color": "white"},
            },
        )
    )
else:
    st.warning("No mapped records found for the current filters.")

st.markdown("## Executive Priority Intelligence")

display_cols = [
    "strategic_priority",
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
