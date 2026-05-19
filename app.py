import os
import pandas as pd
import streamlit as st
import psycopg2
import pydeck as pdk
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Allen Hammett AI",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATABASE_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))
PASSWORD_RESET_KEY = st.secrets.get(
    "PASSWORD_RESET_KEY",
    os.getenv("PASSWORD_RESET_KEY", "AH_RESET_2026"),
)


# =====================================================
# DATABASE
# =====================================================

def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


def run_execute(sql: str, params=None):
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        cur.close()
    finally:
        conn.close()


def table_exists(table_name: str) -> bool:
    df = run_query(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'public'
          and table_name = %s
        """,
        (table_name,),
    )
    return not df.empty


def get_columns(table_name: str):
    if not table_exists(table_name):
        return set()

    df = run_query(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
        """,
        (table_name,),
    )
    return set(df["column_name"].tolist())


# =====================================================
# CLEAN DISPLAY HELPERS
# =====================================================

def clean_value(value, fallback="N/A"):
    if value is None:
        return fallback

    try:
        if pd.isna(value):
            return fallback
    except Exception:
        pass

    text = str(value).strip()

    if text.lower() in ["nan", "none", "null", ""]:
        return fallback

    return text


def clean_bool(value):
    if value in [True, "true", "True", "TRUE", 1, "1"]:
        return "Yes"
    if value in [False, "false", "False", "FALSE", 0, "0"]:
        return "No"
    return "Unknown"


# =====================================================
# AUTH
# =====================================================

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


def reset_password(email, reset_key, new_password):
    if reset_key != PASSWORD_RESET_KEY:
        return False, "Invalid reset key."

    if len(new_password) < 10:
        return False, "Password must be at least 10 characters."

    run_execute(
        """
        update users
        set password = %s
        where lower(email) = lower(%s)
        """,
        (new_password, email.strip()),
    )

    return True, "Password updated. You can now log in."


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

    with st.expander("Forgot password / Reset password"):
        reset_email = st.text_input("Account Email", key="reset_email")
        reset_key = st.text_input("Reset Key", type="password", key="reset_key")
        new_password = st.text_input("New Password", type="password", key="new_password")

        if st.button("Reset Password"):
            success, message = reset_password(reset_email, reset_key, new_password)
            if success:
                st.success(message)
            else:
                st.error(message)

    st.stop()


# =====================================================
# SCORING
# =====================================================

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


def map_color(stage):
    colors = {
        "Prime Positioning": [0, 180, 120, 220],
        "Strategic Development": [20, 80, 180, 220],
        "Active Monitoring": [0, 180, 200, 220],
        "Early Identification": [100, 120, 140, 200],
        "Historical Context": [55, 60, 70, 170],
    }
    return colors.get(stage, [100, 100, 100, 160])


# =====================================================
# DATA LOADERS
# =====================================================

def get_projects(time_horizon):
    if not table_exists("projects"):
        return pd.DataFrame()

    interval_map = {
        "30 Days": "30 days",
        "90 Days": "90 days",
        "12 Months": "12 months",
        "24 Months": "24 months",
    }

    where_clause = ""

    if time_horizon in interval_map:
        where_clause = f"where created_at >= now() - interval '{interval_map[time_horizon]}'"

    df = run_query(
        f"""
        select *
        from projects
        {where_clause}
        order by early_capture_score desc nulls last, created_at desc
        limit 5000
        """
    )

    if df.empty:
        return df

    if "early_capture_score" not in df.columns:
        df["early_capture_score"] = 0

    df["early_capture_score"] = pd.to_numeric(
        df["early_capture_score"],
        errors="coerce",
    ).fillna(0).astype(int)

    df["capture_stage"] = df["early_capture_score"].apply(capture_stage)
    df["map_color"] = df["capture_stage"].apply(map_color)

    return df


def get_relationship_pipeline():
    if not table_exists("executive_project_matches"):
        return pd.DataFrame()

    return run_query(
        """
        select
            canonical_project_name,
            company,
            full_name,
            title,
            email,
            linkedin_url,
            infrastructure_type,
            intelligence_category,
            county,
            market_cluster,
            corridor_region,
            early_capture_score
        from executive_project_matches
        order by early_capture_score desc nulls last,
                 company,
                 full_name
        limit 1000
        """
    )


def get_project_contacts(project_name):
    if not table_exists("executive_project_matches"):
        return pd.DataFrame()

    return run_query(
        """
        select
            full_name,
            title,
            company,
            email,
            linkedin_url
        from executive_project_matches
        where lower(canonical_project_name) = lower(%s)
        order by company, full_name
        limit 25
        """,
        (project_name,),
    )


# =====================================================
# APP START
# =====================================================

user = login_gate()

st.title("Infrastructure Intelligence Platform")
st.markdown("Allen Hammett AI — Executive Infrastructure / Early Capture Intelligence")

# =====================================================
# SIDEBAR FILTERS
# =====================================================

st.sidebar.header("Executive Filters")

time_horizon = st.sidebar.selectbox(
    "Intelligence Timeline",
    ["30 Days", "90 Days", "12 Months", "24 Months", "All Intelligence"],
    index=4,
)

projects_df = get_projects(time_horizon)
relationship_df = get_relationship_pipeline()

filtered_df = projects_df.copy()

search = st.sidebar.text_input("Search keyword")
predictive_only = st.sidebar.checkbox("Predictive signals only")
prime_only = st.sidebar.checkbox("Prime positioning only")

if not filtered_df.empty:
    if search:
        filtered_df = filtered_df[
            filtered_df.astype(str).apply(
                lambda row: row.str.contains(search, case=False, na=False).any(),
                axis=1,
            )
        ]

    if predictive_only and "predictive_signal" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["predictive_signal"] == True]

    if prime_only:
        filtered_df = filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]

    def sidebar_filter(label, column):
        global filtered_df

        if column not in filtered_df.columns:
            return

        options = ["All"] + sorted(
            filtered_df[column]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        selected = st.sidebar.selectbox(label, options)

        if selected != "All":
            filtered_df = filtered_df[filtered_df[column].astype(str) == selected]

    sidebar_filter("County", "county")
    sidebar_filter("Intelligence Category", "intelligence_category")
    sidebar_filter("Capture Stage", "capture_stage")
    sidebar_filter("Corridor Region", "corridor_region")
    sidebar_filter("Project Stage", "project_stage")


# =====================================================
# LEGEND + METRICS
# =====================================================

st.markdown("### Capture Intelligence Legend")

c1, c2, c3, c4, c5 = st.columns(5)
c1.success("Prime Positioning 90–100")
c2.info("Strategic Development 75–89")
c3.markdown("**Active Monitoring**  \n50–74")
c4.markdown("**Early Identification**  \n25–49")
c5.markdown("**Historical Context**  \n0–24")

mapped_count = 0

if not filtered_df.empty and {"latitude", "longitude"}.issubset(filtered_df.columns):
    mapped_count = len(filtered_df.dropna(subset=["latitude", "longitude"]))

m1, m2, m3, m4, m5 = st.columns(5)

m1.metric("Qualified Signals", len(filtered_df))
m2.metric(
    "Prime Positioning",
    len(filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]) if not filtered_df.empty else 0,
)
m3.metric(
    "Predictive Signals",
    len(filtered_df[filtered_df["predictive_signal"] == True]) if "predictive_signal" in filtered_df.columns else 0,
)
m4.metric("Mapped Records", mapped_count)
m5.metric("Executive Relationships", len(relationship_df))


# =====================================================
# MAP
# =====================================================

st.markdown("## Infrastructure Intelligence Map")

if not filtered_df.empty and {"latitude", "longitude"}.issubset(filtered_df.columns):
    map_df = filtered_df.dropna(subset=["latitude", "longitude"]).copy()

    if not map_df.empty:
        map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
        map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
        map_df = map_df.dropna(subset=["latitude", "longitude"])

        st.pydeck_chart(
            pdk.Deck(
                map_style=pdk.map_styles.CARTO_DARK,
                initial_view_state=pdk.ViewState(
                    latitude=float(map_df["latitude"].median()),
                    longitude=float(map_df["longitude"].median()),
                    zoom=9,
                    pitch=35,
                ),
                layers=[
                    pdk.Layer(
                        "ScatterplotLayer",
                        data=map_df,
                        get_position="[longitude, latitude]",
                        get_color="map_color",
                        get_radius=1000,
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
                    "style": {"backgroundColor": "black", "color": "white"},
                },
            )
        )
    else:
        st.warning("No mapped records found for current filters.")
else:
    st.warning("Map fields are not available yet.")


# =====================================================
# EXECUTIVE INTELLIGENCE PROFILES
# =====================================================

st.markdown("## Executive Intelligence Profiles")

if filtered_df.empty:
    st.info("No infrastructure intelligence records match the current filters.")

else:
    for _, row in filtered_df.head(100).iterrows():
        project_name = clean_value(row.get("canonical_project_name"), "Unnamed Project")
        stage = clean_value(row.get("capture_stage"), "Unknown")

        with st.expander(f"{project_name} ({stage})"):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"**Case Number:** {clean_value(row.get('case_number'))}")
                st.markdown(f"**County:** {clean_value(row.get('county'))}")
                st.markdown(f"**Project Stage:** {clean_value(row.get('project_stage'))}")
                st.markdown(f"**Infrastructure Type:** {clean_value(row.get('infrastructure_type'))}")
                st.markdown(f"**Intelligence Category:** {clean_value(row.get('intelligence_category'))}")
                st.markdown(f"**Capture Score:** {clean_value(row.get('early_capture_score'))}")
                st.markdown(f"**Applicant:** {clean_value(row.get('applicant_name'), 'Unknown')}")
                st.markdown(f"**Utility Provider:** {clean_value(row.get('utility_dependency'), 'Unknown')}")

            with col2:
                st.markdown(f"**Corridor Region:** {clean_value(row.get('corridor_region'))}")
                st.markdown(f"**Market Cluster:** {clean_value(row.get('market_cluster'))}")
                st.markdown(f"**Filing Date:** {clean_value(row.get('filing_date'))}")
                st.markdown(f"**Source Name:** {clean_value(row.get('source_name'))}")
                st.markdown(f"**Source Type:** {clean_value(row.get('source_type'))}")
                st.markdown(f"**Predictive Signal:** {clean_bool(row.get('predictive_signal'))}")
                st.markdown(f"**Estimated MW Demand:** {clean_value(row.get('estimated_power_mw'), 'Unknown')}")
                st.markdown(f"**Created At:** {clean_value(row.get('created_at'))}")

            st.divider()

            st.subheader("Executive Strategic Assessment")
            st.info(clean_value(row.get("strategic_notes"), "No strategic assessment generated yet."))

            st.subheader("Infrastructure Risk Flags")
            risk_flags = clean_value(row.get("risk_flags"), "")

            if risk_flags:
                for flag in str(risk_flags).split(","):
                    cleaned_flag = flag.strip()
                    if cleaned_flag:
                        st.warning(cleaned_flag)
            else:
                st.success("No major infrastructure risks currently detected.")

            st.subheader("Relationship Intelligence")

            contacts_df = get_project_contacts(project_name)

            if not contacts_df.empty:
                st.success(f"{len(contacts_df)} matched executive contacts identified")

                for _, contact in contacts_df.iterrows():
                    st.markdown(
                        f"""
                        **{clean_value(contact.get('full_name'), 'Unnamed Contact')}**  
                        **Title:** {clean_value(contact.get('title'))}  
                        **Company:** {clean_value(contact.get('company'))}  
                        **Email:** {clean_value(contact.get('email'), 'Not provided')}
                        """
                    )

                    linkedin_url = clean_value(contact.get("linkedin_url"), "")

                    if linkedin_url:
                        st.markdown(f"[LinkedIn Profile]({linkedin_url})")

                    st.divider()
            else:
                st.info("No executive relationship intelligence matched yet.")

            st.subheader("Permit Description")
            st.write(clean_value(row.get("permit_description"), "No permit description stored yet."))

            st.subheader("Raw Filing Intelligence")
            st.code(clean_value(row.get("raw_text"), "No raw filing text stored yet.")[:5000])


# =====================================================
# EXECUTIVE RELATIONSHIP PIPELINE
# =====================================================

st.markdown("## Executive Relationship Pipeline")

if relationship_df.empty:
    st.info("No executive relationship intelligence found.")

else:
    st.success(f"{len(relationship_df)} executive relationships mapped")

    relationship_search = st.text_input("Search executive relationships")

    display_relationship_df = relationship_df.copy()

    if relationship_search:
        display_relationship_df = display_relationship_df[
            display_relationship_df.astype(str).apply(
                lambda row: row.str.contains(
                    relationship_search,
                    case=False,
                    na=False,
                ).any(),
                axis=1,
            )
        ]

    st.dataframe(
        display_relationship_df,
        use_container_width=True,
        height=500,
    )

    st.download_button(
        "Download Executive Relationship Pipeline CSV",
        display_relationship_df.to_csv(index=False),
        "executive_relationship_pipeline.csv",
        "text/csv",
    )


# =====================================================
# EXECUTIVE PRIORITY TABLE
# =====================================================

st.markdown("## Executive Priority Intelligence")

priority_cols = [
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
    "created_at",
]

existing_priority_cols = [c for c in priority_cols if c in filtered_df.columns]

if not filtered_df.empty:
    st.dataframe(filtered_df[existing_priority_cols], use_container_width=True)
else:
    st.info("No records to display.")


# =====================================================
# EXPORTS
# =====================================================

st.markdown("## Export Intelligence")

if not filtered_df.empty:
    st.download_button(
        "Download Opportunities CSV",
        filtered_df.to_csv(index=False),
        "infrastructure_opportunities.csv",
        "text/csv",
    )

if not relationship_df.empty:
    st.download_button(
        "Download All Executive Relationships CSV",
        relationship_df.to_csv(index=False),
        "all_executive_relationships.csv",
        "text/csv",
    )


# =====================================================
# SIGN OUT
# =====================================================

if st.button("Sign Out"):
    st.session_state.user = None
    st.rerun()

st.caption("Allen Hammett AI • Infrastructure Intelligence + Relationship Capture System")