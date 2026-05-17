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

DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL)


# -----------------------------
# DATABASE QUERY FUNCTION
# -----------------------------

def run_query(query):

    return pd.read_sql(query, conn)


# -----------------------------
# LOGIN SYSTEM
# -----------------------------

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if "user_role" not in st.session_state:
    st.session_state.user_role = None


def authenticate(email, password):

    query = f"""
    SELECT role
    FROM users
    WHERE email = '{email}'
    AND password = '{password}'
    LIMIT 1
    """

    result = run_query(query)

    if len(result) > 0:
        return result.iloc[0]["role"]

    return None


if not st.session_state.authenticated:

    st.title("Allen Hammett AI")

    st.subheader("Secure Infrastructure Intelligence Access")

    with st.form("login_form"):

        email = st.text_input("Email")

        password = st.text_input(
            "Password",
            type="password"
        )

        submitted = st.form_submit_button("Login")

    if submitted:

        role = authenticate(email, password)

        if role:

            st.session_state.authenticated = True
            st.session_state.user_role = role

            st.rerun()

        else:
            st.error("Invalid credentials")

    st.stop()


# -----------------------------
# DASHBOARD HEADER
# -----------------------------

st.title("Infrastructure Intelligence Platform")

st.markdown("""
Allen Hammett AI — Private Infrastructure / Data Center Intelligence
""")


# -----------------------------
# DASHBOARD METRICS
# -----------------------------

projects_count = run_query("""
SELECT COUNT(*) as total
FROM projects
""")

leads_count = run_query("""
SELECT COUNT(*) as total
FROM leads
""")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "Qualified Opportunities",
        int(projects_count.iloc[0]["total"])
    )

with col2:
    st.metric(
        "Infrastructure Leads",
        int(leads_count.iloc[0]["total"])
    )

with col3:
    st.metric(
        "Active User",
        st.session_state.user_role
    )


# -----------------------------
# PROJECTS QUERY
# -----------------------------

projects_df = run_query("""

SELECT
    case_number,
    canonical_project_name,
    project_stage,
    project_type,
    county,
    state,
    created_at,
    latitude,
    longitude
FROM projects
WHERE (
    canonical_project_name ILIKE '%data center%'
    OR canonical_project_name ILIKE '%substation%'
    OR canonical_project_name ILIKE '%utility%'
    OR canonical_project_name ILIKE '%transmission%'
    OR canonical_project_name ILIKE '%electric%'
    OR canonical_project_name ILIKE '%energy%'
)
AND latitude IS NOT NULL
AND longitude IS NOT NULL
ORDER BY created_at DESC
LIMIT 250

""")


# -----------------------------
# MAP SECTION
# -----------------------------

st.header("Infrastructure Intelligence Map")

if len(projects_df) > 0:

    map_df = projects_df.rename(columns={
        "latitude": "lat",
        "longitude": "lon"
    })

    st.pydeck_chart(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/dark-v10",

            initial_view_state=pdk.ViewState(
                latitude=39.0438,
                longitude=-77.4874,
                zoom=8,
                pitch=45
            ),

            layers=[

                pdk.Layer(
                    "ScatterplotLayer",
                    data=map_df,

                    get_position="[lon, lat]",

                    get_color="[0, 255, 140, 180]",

                    get_radius=1200,

                    pickable=True,

                    auto_highlight=True
                )

            ],

            tooltip={
                "html": """
                <b>Project:</b> {canonical_project_name}<br/>
                <b>Type:</b> {project_type}<br/>
                <b>County:</b> {county}<br/>
                <b>Stage:</b> {project_stage}
                """,

                "style": {
                    "backgroundColor": "black",
                    "color": "white"
                }
            }
        )
    )

else:

    st.warning("No geospatial infrastructure records found.")


# -----------------------------
# TOP OPPORTUNITIES
# -----------------------------

st.header("Top Infrastructure Opportunities")

display_projects = projects_df[[
    "case_number",
    "canonical_project_name",
    "project_stage",
    "project_type",
    "county",
    "state",
    "created_at"
]]

st.dataframe(
    display_projects,
    use_container_width=True
)


# -----------------------------
# LEADS
# -----------------------------

leads_df = run_query("""

SELECT
    company,
    contact_name,
    title,
    email,
    phone,
    county,
    created_at
FROM leads
ORDER BY created_at DESC
LIMIT 100

""")


st.header("Infrastructure Leads")

st.dataframe(
    leads_df,
    use_container_width=True
)


# -----------------------------
# EXPORT
# -----------------------------

st.header("Export Leads")

csv = leads_df.to_csv(index=False)

st.download_button(
    label="Download Leads CSV",
    data=csv,
    file_name="infrastructure_leads.csv",
    mime="text/csv"
)


# -----------------------------
# SIGN OUT
# -----------------------------

if st.button("Sign Out"):

    st.session_state.authenticated = False
    st.session_state.user_role = None

    st.rerun()


st.caption(
    "Allen Hammett AI • Infrastructure Intelligence System"
)
