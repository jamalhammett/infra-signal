import os
import pandas as pd
import streamlit as st
import psycopg2
import pydeck as pdk
from dotenv import load_dotenv
from datetime import datetime

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
    st.subheader("Institutional Infrastructure Intelligence Access")

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
# DATA LOADERS
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
# SESSION STATE
# =====================================================

if "watchlist" not in st.session_state:
    st.session_state.watchlist = []

if "selected_project" not in st.session_state:

    if not projects_df.empty:
        st.session_state.selected_project = projects_df.iloc[0][
            "canonical_project_name"
        ]

    else:
        st.session_state.selected_project = None

# =====================================================
# SIDEBAR
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

if watchlist_only and len(st.session_state.watchlist) > 0:

    filtered_df = filtered_df[
        filtered_df["canonical_project_name"].isin(
            st.session_state.watchlist
        )
    ]

# =====================================================
# HEADER
# =====================================================

st.title("Infrastructure Intelligence Operating System")

st.markdown(
    """
    Allen Hammett AI — Institutional Infrastructure Intelligence + Relationship Intelligence
    """
)

# =====================================================
# KPI ROW
# =====================================================

k1, k2, k3, k4, k5, k6 = st.columns(6)

k1.metric(
    "Signals",
    len(filtered_df),
)

k2.metric(
    "Prime",
    len(
        filtered_df[
            filtered_df["capture_stage"] == "Prime Positioning"
        ]
    ),
)

k3.metric(
    "Predictive",
    len(
        filtered_df[
            filtered_df["predictive_signal"] == True
        ]
    ) if "predictive_signal" in filtered_df.columns else 0,
)

k4.metric(
    "Relationships",
    len(relationships_df),
)

k5.metric(
    "Watchlist",
    len(st.session_state.watchlist),
)

k6.metric(
    "Mapped",
    len(
        filtered_df.dropna(
            subset=["latitude", "longitude"]
        )
    ) if "latitude" in filtered_df.columns else 0,
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
# MAIN LAYOUT
# =====================================================

left_panel, right_panel = st.columns([1.2, 2.8])

# =====================================================
# LEFT PANEL
# =====================================================

with left_panel:

    st.markdown("## Priority Infrastructure Queue")

    quick_search = st.text_input(
        "Quick Project Filter"
    )

    queue_df = filtered_df.copy()

    if quick_search:

        queue_df = queue_df[
            queue_df["canonical_project_name"].astype(str).str.contains(
                quick_search,
                case=False,
                na=False,
            )
        ]

    queue_df = queue_df.sort_values(
        by="early_capture_score",
        ascending=False,
    )

    queue_limit = st.slider(
        "Visible Queue Size",
        10,
        200,
        40,
    )

    queue_df = queue_df.head(queue_limit)

    for _, project in queue_df.iterrows():

        project_name = clean_value(
            project.get("canonical_project_name")
        )

        score = clean_value(
            project.get("early_capture_score")
        )

        stage = clean_value(
            project.get("capture_stage")
        )

        if st.button(
            f"{project_name} | {score}",
            use_container_width=True,
        ):

            st.session_state.selected_project = project_name

    st.markdown("---")

    st.markdown("## Watchlist")

    if st.button(
        "⭐ Add Current Project",
        use_container_width=True,
    ):

        current_project = st.session_state.selected_project

        if current_project not in st.session_state.watchlist:
            st.session_state.watchlist.append(current_project)

    if len(st.session_state.watchlist) == 0:

        st.info("No watchlist projects yet.")

    else:

        for watch_item in st.session_state.watchlist:

            watch_col1, watch_col2 = st.columns([5,1])

            with watch_col1:

                if st.button(
                    watch_item,
                    key=f"watch_{watch_item}",
                    use_container_width=True,
                ):

                    st.session_state.selected_project = watch_item

            with watch_col2:

                if st.button(
                    "❌",
                    key=f"remove_{watch_item}",
                ):

                    st.session_state.watchlist.remove(watch_item)
                    st.rerun()

# =====================================================
# RIGHT PANEL
# =====================================================

with right_panel:

    selected_project = st.session_state.selected_project

    selected_df = filtered_df[
        filtered_df["canonical_project_name"] == selected_project
    ]

    if not selected_df.empty:

        row = selected_df.iloc[0]

        # =============================================
        # EXECUTIVE SNAPSHOT HEADER
        # =============================================

        st.markdown(f"# {selected_project}")

        s1, s2, s3, s4, s5 = st.columns(5)

        s1.metric(
            "Capture Score",
            clean_value(row.get("early_capture_score"))
        )

        s2.metric(
            "Stage",
            clean_value(row.get("capture_stage"))
        )

        s3.metric(
            "MW Demand",
            clean_value(row.get("estimated_power_mw"))
        )

        s4.metric(
            "Relationships",
            len(
                relationships_df[
                    relationships_df["canonical_project_name"]
                    == selected_project
                ]
            )
        )

        s5.metric(
            "Predictive",
            clean_bool(
                row.get("predictive_signal")
            )
        )

        # =============================================
        # LIVE SIGNAL FEED
        # =============================================

        st.markdown("## Live Intelligence Feed")

        feed_items = []

        if clean_bool(row.get("predictive_signal")) == "Yes":
            feed_items.append(
                "Predictive infrastructure signal identified"
            )

        if clean_value(row.get("estimated_power_mw")) != "N/A":
            feed_items.append(
                f"Estimated MW demand detected: {clean_value(row.get('estimated_power_mw'))}"
            )

        if clean_value(row.get("utility_dependency")) != "N/A":
            feed_items.append(
                f"Utility dependency mapped: {clean_value(row.get('utility_dependency'))}"
            )

        if clean_value(row.get("risk_flags")) != "N/A":
            feed_items.append(
                f"Infrastructure risks identified: {clean_value(row.get('risk_flags'))}"
            )

        feed_items.append(
            f"Project currently in {clean_value(row.get('capture_stage'))} stage"
        )

        for item in feed_items:
            st.info(item)

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

            c1, c2 = st.columns(2)

            with c1:

                st.markdown(
                    f"**Infrastructure Type:** {clean_value(row.get('infrastructure_type'))}"
                )

                st.markdown(
                    f"**Project Stage:** {clean_value(row.get('project_stage'))}"
                )

                st.markdown(
                    f"**County:** {clean_value(row.get('county'))}"
                )

                st.markdown(
                    f"**Corridor Region:** {clean_value(row.get('corridor_region'))}"
                )

            with c2:

                st.markdown(
                    f"**Market Cluster:** {clean_value(row.get('market_cluster'))}"
                )

                st.markdown(
                    f"**Utility Provider:** {clean_value(row.get('utility_dependency'))}"
                )

                st.markdown(
                    f"**Case Number:** {clean_value(row.get('case_number'))}"
                )

                st.markdown(
                    f"**Created:** {clean_value(row.get('created_at'))}"
                )

        # =============================================
        # STRATEGIC
        # =============================================

        with strategy_tab:

            st.markdown("## Executive Strategic Assessment")

            st.info(
                clean_value(
                    row.get("strategic_notes"),
                    "No strategic assessment generated yet."
                )
            )

            st.markdown("## Infrastructure Risk Flags")

            risk_flags = clean_value(
                row.get("risk_flags"),
                ""
            )

            if risk_flags == "":
                st.success(
                    "No critical infrastructure risks detected."
                )

            else:

                for risk in str(risk_flags).split(","):
                    st.warning(risk.strip())

        # =============================================
        # RELATIONSHIPS
        # =============================================

        with relationship_tab:

            st.markdown("## Executive Relationship Intelligence")

            project_relationships = relationships_df[
                relationships_df["canonical_project_name"]
                == selected_project
            ]

            if project_relationships.empty:

                st.warning(
                    "No executive relationships identified."
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
