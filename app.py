import os
import json
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

DATABASE_URL = st.secrets.get(
    "DATABASE_URL",
    os.getenv("DATABASE_URL"),
)

PASSWORD_RESET_KEY = st.secrets.get(
    "PASSWORD_RESET_KEY",
    os.getenv("PASSWORD_RESET_KEY", "AH_RESET_2026"),
)

# =====================================================
# DATABASE
# =====================================================

def run_query(sql, params=None):
    conn = psycopg2.connect(DATABASE_URL)

    try:
        return pd.read_sql(sql, conn, params=params)

    finally:
        conn.close()


def run_execute(sql, params=None):
    conn = psycopg2.connect(DATABASE_URL)

    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        cur.close()

    finally:
        conn.close()


# =====================================================
# HELPERS
# =====================================================

def clean_value(value, fallback="N/A"):

    if value is None:
        return fallback

    try:
        if pd.isna(value):
            return fallback
    except:
        pass

    text = str(value).strip()

    if text.lower() in ["nan", "none", "null", ""]:
        return fallback

    return text


def clean_bool(value):

    if value in [True, "true", "True", 1]:
        return "Yes"

    if value in [False, "false", "False", 0]:
        return "No"

    return "Unknown"


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
        "Prime Positioning": [0, 180, 120],
        "Strategic Development": [20, 80, 180],
        "Active Monitoring": [0, 180, 200],
        "Early Identification": [140, 140, 140],
        "Historical Context": [90, 90, 90],
    }

    return colors.get(stage, [100, 100, 100])


# =====================================================
# AUTH
# =====================================================

def authenticate(email, password):

    df = run_query(
        """
        select *
        from users
        where lower(email)=lower(%s)
        and password=%s
        limit 1
        """,
        (email.strip(), password),
    )

    if df.empty:
        return None

    return df.iloc[0].to_dict()


def reset_password(email, reset_key, new_password):

    if reset_key != PASSWORD_RESET_KEY:
        return False, "Invalid reset key"

    run_execute(
        """
        update users
        set password=%s
        where lower(email)=lower(%s)
        """,
        (new_password, email.strip()),
    )

    return True, "Password updated successfully"


def login_gate():

    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user:
        return

    st.title("Allen Hammett AI")
    st.subheader("Executive Infrastructure Intelligence Access")

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

    with st.expander("Forgot Password / Reset Password"):

        reset_email = st.text_input("Account Email")
        reset_key = st.text_input("Reset Key", type="password")
        new_password = st.text_input("New Password", type="password")

        if st.button("Reset Password"):

            success, message = reset_password(
                reset_email,
                reset_key,
                new_password,
            )

            if success:
                st.success(message)

            else:
                st.error(message)

    st.stop()


# =====================================================
# LOAD DATA
# =====================================================

@st.cache_data(ttl=300)
def load_projects():

    df = run_query(
        """
        select *
        from projects
        order by early_capture_score desc nulls last,
                 created_at desc
        limit 5000
        """
    )

    if "early_capture_score" not in df.columns:
        df["early_capture_score"] = 0

    df["early_capture_score"] = pd.to_numeric(
        df["early_capture_score"],
        errors="coerce"
    ).fillna(0)

    df["capture_stage"] = df[
        "early_capture_score"
    ].apply(capture_stage)

    df["map_color"] = df[
        "capture_stage"
    ].apply(map_color)

    return df


@st.cache_data(ttl=300)
def load_relationships():

    return run_query(
        """
        select *
        from executive_project_matches
        order by early_capture_score desc nulls last
        """
    )


projects_df = load_projects()
relationships_df = load_relationships()

# =====================================================
# LOGIN
# =====================================================

login_gate()

# =====================================================
# GLOBAL SEARCH
# =====================================================

st.sidebar.header("Executive Command Filters")

global_search = st.sidebar.text_input(
    "Global Intelligence Search"
)

prime_only = st.sidebar.checkbox(
    "Prime Positioning Only"
)

predictive_only = st.sidebar.checkbox(
    "Predictive Signals Only"
)

watchlist_only = st.sidebar.checkbox(
    "Watchlist Only"
)

filtered_df = projects_df.copy()

if global_search:

    filtered_df = filtered_df[
        filtered_df.astype(str).apply(
            lambda row: row.str.contains(
                global_search,
                case=False,
                na=False,
            ).any(),
            axis=1,
        )
    ]

if prime_only:

    filtered_df = filtered_df[
        filtered_df["capture_stage"] == "Prime Positioning"
    ]

if predictive_only and "predictive_signal" in filtered_df.columns:

    filtered_df = filtered_df[
        filtered_df["predictive_signal"] == True
    ]

# =====================================================
# WATCHLIST STATE
# =====================================================

if "watchlist" not in st.session_state:
    st.session_state.watchlist = []

# =====================================================
# HEADER
# =====================================================

st.title("Infrastructure Intelligence Command Center")

st.markdown(
    """
    Allen Hammett AI — Institutional Infrastructure Intelligence Operating System
    """
)

# =====================================================
# KPI ROW
# =====================================================

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric(
    "Qualified Signals",
    len(filtered_df),
)

c2.metric(
    "Prime Positioning",
    len(
        filtered_df[
            filtered_df["capture_stage"] == "Prime Positioning"
        ]
    ),
)

c3.metric(
    "Predictive Signals",
    len(
        filtered_df[
            filtered_df["predictive_signal"] == True
        ]
    ) if "predictive_signal" in filtered_df.columns else 0,
)

c4.metric(
    "Mapped Projects",
    len(
        filtered_df.dropna(
            subset=["latitude", "longitude"]
        )
    ) if "latitude" in filtered_df.columns else 0,
)

c5.metric(
    "Executive Relationships",
    len(relationships_df),
)

# =====================================================
# MAP
# =====================================================

st.markdown("## Infrastructure Intelligence Map")

if "latitude" in filtered_df.columns:

    map_df = filtered_df.dropna(
        subset=["latitude", "longitude"]
    ).copy()

    if not map_df.empty:

        st.pydeck_chart(
            pdk.Deck(
                map_style=pdk.map_styles.CARTO_DARK,
                initial_view_state=pdk.ViewState(
                    latitude=float(map_df["latitude"].median()),
                    longitude=float(map_df["longitude"].median()),
                    zoom=8,
                    pitch=35,
                ),
                layers=[
                    pdk.Layer(
                        "ScatterplotLayer",
                        data=map_df,
                        get_position="[longitude, latitude]",
                        get_radius=1800,
                        get_fill_color="map_color",
                        pickable=True,
                    )
                ],
                tooltip={
                    "html": """
                    <b>Project:</b> {canonical_project_name}<br/>
                    <b>Capture Stage:</b> {capture_stage}<br/>
                    <b>Score:</b> {early_capture_score}<br/>
                    <b>Category:</b> {intelligence_category}<br/>
                    <b>County:</b> {county}
                    """,
                    "style": {
                        "backgroundColor": "black",
                        "color": "white",
                    },
                },
            )
        )

# =====================================================
# MASTER DETAIL LAYOUT
# =====================================================

left_col, right_col = st.columns([1.1, 2.2])

# =====================================================
# LEFT PANEL
# =====================================================

with left_col:

    st.markdown("## Priority Projects")

    project_display_df = filtered_df[
        [
            "canonical_project_name",
            "capture_stage",
            "early_capture_score",
        ]
    ].copy()

    project_display_df = project_display_df.fillna("N/A")

    project_options = project_display_df[
        "canonical_project_name"
    ].tolist()

    selected_project = st.radio(
        "Select Project",
        project_options,
        label_visibility="collapsed",
    )

    st.markdown("---")

    st.markdown("## Watchlist")

    if st.button("⭐ Add Selected Project"):

        if selected_project not in st.session_state.watchlist:
            st.session_state.watchlist.append(selected_project)

    if len(st.session_state.watchlist) == 0:

        st.info("No watchlist projects yet.")

    else:

        for watch_item in st.session_state.watchlist:
            st.success(watch_item)

# =====================================================
# RIGHT PANEL
# =====================================================

with right_col:

    selected_df = filtered_df[
        filtered_df["canonical_project_name"] == selected_project
    ]

    if not selected_df.empty:

        row = selected_df.iloc[0]

        st.markdown(f"# {selected_project}")

        stage_color = {
            "Prime Positioning": "🟢",
            "Strategic Development": "🔵",
            "Active Monitoring": "🟡",
            "Early Identification": "⚪",
            "Historical Context": "⚫",
        }

        st.markdown(
            f"""
            {stage_color.get(row['capture_stage'], '⚪')}
            {row['capture_stage']}
            """
        )

        # =============================================
        # TABS
        # =============================================

        overview_tab, strategy_tab, relationship_tab, permit_tab, raw_tab = st.tabs([
            "Overview",
            "Strategic Assessment",
            "Relationships",
            "Permit Intelligence",
            "Raw Intelligence",
        ])

        # =============================================
        # OVERVIEW
        # =============================================

        with overview_tab:

            col1, col2 = st.columns(2)

            with col1:

                st.markdown(
                    f"**Capture Score:** {clean_value(row.get('early_capture_score'))}"
                )

                st.markdown(
                    f"**Infrastructure Type:** {clean_value(row.get('infrastructure_type'))}"
                )

                st.markdown(
                    f"**Project Stage:** {clean_value(row.get('project_stage'))}"
                )

                st.markdown(
                    f"**Case Number:** {clean_value(row.get('case_number'))}"
                )

                st.markdown(
                    f"**County:** {clean_value(row.get('county'))}"
                )

            with col2:

                st.markdown(
                    f"**Market Cluster:** {clean_value(row.get('market_cluster'))}"
                )

                st.markdown(
                    f"**Corridor Region:** {clean_value(row.get('corridor_region'))}"
                )

                st.markdown(
                    f"**Estimated MW Demand:** {clean_value(row.get('estimated_power_mw'))}"
                )

                st.markdown(
                    f"**Predictive Signal:** {clean_bool(row.get('predictive_signal'))}"
                )

                st.markdown(
                    f"**Created:** {clean_value(row.get('created_at'))}"
                )

        # =============================================
        # STRATEGIC
        # =============================================

        with strategy_tab:

            st.markdown("## Executive Strategic Assessment")

            strategic_notes = clean_value(
                row.get("strategic_notes"),
                "No strategic assessment generated."
            )

            st.info(strategic_notes)

            st.markdown("## Infrastructure Risk Flags")

            risk_flags = clean_value(
                row.get("risk_flags"),
                ""
            )

            if risk_flags == "":
                st.success("No major risk flags identified.")

            else:

                for risk in str(risk_flags).split(","):
                    st.warning(risk.strip())

        # =============================================
        # RELATIONSHIPS
        # =============================================

        with relationship_tab:

            st.markdown("## Executive Relationship Intelligence")

            project_relationships = relationships_df[
                relationships_df["canonical_project_name"] == selected_project
            ]

            if project_relationships.empty:

                st.warning(
                    "No executive relationships mapped yet."
                )

            else:

                st.success(
                    f"{len(project_relationships)} executive relationships identified"
                )

                relationship_columns = [
                    "full_name",
                    "title",
                    "company",
                    "email",
                    "linkedin_url",
                ]

                existing_cols = [
                    c for c in relationship_columns
                    if c in project_relationships.columns
                ]

                st.dataframe(
                    project_relationships[existing_cols],
                    use_container_width=True,
                    height=450,
                )

        # =============================================
        # PERMIT
        # =============================================

        with permit_tab:

            st.markdown("## Permit Intelligence")

            st.markdown(
                f"**Applicant:** {clean_value(row.get('applicant_name'))}"
            )

            st.markdown(
                f"**Source Name:** {clean_value(row.get('source_name'))}"
            )

            st.markdown(
                f"**Source Type:** {clean_value(row.get('source_type'))}"
            )

            st.markdown(
                f"**Filing Date:** {clean_value(row.get('filing_date'))}"
            )

            st.markdown("### Permit Description")

            st.write(
                clean_value(
                    row.get("permit_description"),
                    "No permit description available."
                )
            )

        # =============================================
        # RAW INTEL
        # =============================================

        with raw_tab:

            st.markdown("## Raw Filing Intelligence")

            raw_text = clean_value(
                row.get("raw_text"),
                "No raw filing intelligence available."
            )

            st.code(raw_text[:10000])

# =====================================================
# RELATIONSHIP COMMAND CENTER
# =====================================================

st.markdown("## Executive Relationship Command Center")

relationship_search = st.text_input(
    "Search Executive Relationships"
)

display_relationships = relationships_df.copy()

if relationship_search:

    display_relationships = display_relationships[
        display_relationships.astype(str).apply(
            lambda row: row.str.contains(
                relationship_search,
                case=False,
                na=False,
            ).any(),
            axis=1,
        )
    ]

relationship_columns = [
    "canonical_project_name",
    "company",
    "full_name",
    "title",
    "email",
]

existing_relationship_cols = [
    c for c in relationship_columns
    if c in display_relationships.columns
]

st.dataframe(
    display_relationships[
        existing_relationship_cols
    ],
    use_container_width=True,
    height=500,
)

# =====================================================
# EXPORTS
# =====================================================

st.markdown("## Export Intelligence")

st.download_button(
    "Download Infrastructure Intelligence CSV",
    filtered_df.to_csv(index=False),
    "infrastructure_intelligence.csv",
    "text/csv",
)

st.download_button(
    "Download Executive Relationship Pipeline CSV",
    relationships_df.to_csv(index=False),
    "executive_relationship_pipeline.csv",
    "text/csv",
)

# =====================================================
# SIGN OUT
# =====================================================

if st.button("Sign Out"):
    st.session_state.user = None
    st.rerun()

st.caption(
    """
    Allen Hammett AI • Institutional Infrastructure Intelligence Operating System
    """
)
