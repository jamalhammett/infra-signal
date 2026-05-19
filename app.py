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
PASSWORD_RESET_KEY = st.secrets.get("PASSWORD_RESET_KEY", os.getenv("PASSWORD_RESET_KEY", "AH_RESET_2026"))


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
    if value in [True, "true", "True", 1, "1"]:
        return "Yes"
    if value in [False, "false", "False", 0, "0"]:
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

    if len(new_password) < 10:
        return False, "Password must be at least 10 characters."

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
        reset_email = st.text_input("Account Email", key="reset_email")
        reset_key = st.text_input("Reset Key", type="password", key="reset_key")
        new_password = st.text_input("New Password", type="password", key="new_password")

        if st.button("Reset Password", key="reset_password_button"):
            success, message = reset_password(reset_email, reset_key, new_password)
            if success:
                st.success(message)
            else:
                st.error(message)

    st.stop()


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

    if df.empty:
        return df

    if "early_capture_score" not in df.columns:
        df["early_capture_score"] = 0

    df["early_capture_score"] = pd.to_numeric(
        df["early_capture_score"],
        errors="coerce"
    ).fillna(0)

    df["capture_stage"] = df["early_capture_score"].apply(capture_stage)
    df["map_color"] = df["capture_stage"].apply(map_color)

    return df


@st.cache_data(ttl=300)
def load_relationships():
    try:
        return run_query(
            """
            select *
            from executive_project_matches
            order by early_capture_score desc nulls last
            """
        )
    except Exception:
        return pd.DataFrame()


login_gate()

projects_df = load_projects()
relationships_df = load_relationships()

if "watchlist" not in st.session_state:
    st.session_state.watchlist = []

if "selected_project" not in st.session_state:
    if not projects_df.empty and "canonical_project_name" in projects_df.columns:
        st.session_state.selected_project = projects_df.iloc[0]["canonical_project_name"]
    else:
        st.session_state.selected_project = None


st.sidebar.header("Executive Command Filters")

global_search = st.sidebar.text_input("Global Intelligence Search")
prime_only = st.sidebar.checkbox("Prime Positioning Only")
predictive_only = st.sidebar.checkbox("Predictive Signals Only")
watchlist_only = st.sidebar.checkbox("Watchlist Only")

filtered_df = projects_df.copy()

if not filtered_df.empty:
    if global_search:
        filtered_df = filtered_df[
            filtered_df.astype(str).apply(
                lambda row: row.str.contains(global_search, case=False, na=False).any(),
                axis=1,
            )
        ]

    if prime_only:
        filtered_df = filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]

    if predictive_only and "predictive_signal" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["predictive_signal"] == True]

    if watchlist_only and st.session_state.watchlist:
        filtered_df = filtered_df[
            filtered_df["canonical_project_name"].isin(st.session_state.watchlist)
        ]


st.title("Infrastructure Intelligence Operating System")
st.markdown("Allen Hammett AI — Institutional Infrastructure Intelligence + Relationship Intelligence")

k1, k2, k3, k4, k5, k6 = st.columns(6)

k1.metric("Signals", len(filtered_df))
k2.metric(
    "Prime",
    len(filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]) if not filtered_df.empty else 0,
)
k3.metric(
    "Predictive",
    len(filtered_df[filtered_df["predictive_signal"] == True]) if not filtered_df.empty and "predictive_signal" in filtered_df.columns else 0,
)
k4.metric("Relationships", len(relationships_df))
k5.metric("Watchlist", len(st.session_state.watchlist))
k6.metric(
    "Mapped",
    len(filtered_df.dropna(subset=["latitude", "longitude"])) if not filtered_df.empty and {"latitude", "longitude"}.issubset(filtered_df.columns) else 0,
)

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
                    "style": {"backgroundColor": "black", "color": "white"},
                },
            )
        )
    else:
        st.warning("No mapped records found for current filters.")


left_panel, right_panel = st.columns([1.2, 2.8])

with left_panel:
    st.markdown("## Priority Infrastructure Queue")

    quick_search = st.text_input("Quick Project Filter", key="quick_project_filter")

    queue_df = filtered_df.copy()

    if not queue_df.empty and quick_search:
        queue_df = queue_df[
            queue_df["canonical_project_name"].astype(str).str.contains(
                quick_search,
                case=False,
                na=False,
            )
        ]

    if not queue_df.empty:
        queue_df = queue_df.sort_values(by="early_capture_score", ascending=False)

    queue_limit = st.slider(
        "Visible Queue Size",
        min_value=10,
        max_value=200,
        value=40,
        key="visible_queue_size",
    )

    queue_df = queue_df.head(queue_limit)

    if queue_df.empty:
        st.info("No projects match current filters.")
    else:
        for idx, project in queue_df.reset_index(drop=True).iterrows():
            project_name = clean_value(project.get("canonical_project_name"), "Unnamed Project")
            score = clean_value(project.get("early_capture_score"), "0")
            project_id = clean_value(project.get("id"), f"row_{idx}")

            if st.button(
                f"{project_name} | {score}",
                use_container_width=True,
                key=f"project_select_{project_id}_{idx}",
            ):
                st.session_state.selected_project = project_name
                st.rerun()

    st.markdown("---")
    st.markdown("## Watchlist")

    if st.button("⭐ Add Current Project", use_container_width=True, key="add_current_project_watchlist"):
        current_project = st.session_state.selected_project
        if current_project and current_project not in st.session_state.watchlist:
            st.session_state.watchlist.append(current_project)
            st.rerun()

    if not st.session_state.watchlist:
        st.info("No watchlist projects yet.")
    else:
        for idx, watch_item in enumerate(list(st.session_state.watchlist)):
            watch_col1, watch_col2 = st.columns([5, 1])

            with watch_col1:
                if st.button(
                    watch_item,
                    key=f"watch_select_{idx}_{watch_item}",
                    use_container_width=True,
                ):
                    st.session_state.selected_project = watch_item
                    st.rerun()

            with watch_col2:
                if st.button(
                    "❌",
                    key=f"watch_remove_{idx}_{watch_item}",
                ):
                    st.session_state.watchlist.remove(watch_item)
                    st.rerun()


with right_panel:
    selected_project = st.session_state.selected_project

    if not selected_project:
        st.info("Select a project from the Priority Infrastructure Queue.")
    else:
        selected_df = filtered_df[
            filtered_df["canonical_project_name"].astype(str) == str(selected_project)
        ]

        if selected_df.empty:
            st.warning("Selected project is not available under current filters.")
        else:
            row = selected_df.iloc[0]

            st.markdown(f"# {selected_project}")

            project_relationships = pd.DataFrame()
            if not relationships_df.empty and "canonical_project_name" in relationships_df.columns:
                project_relationships = relationships_df[
                    relationships_df["canonical_project_name"].astype(str) == str(selected_project)
                ]

            s1, s2, s3, s4, s5 = st.columns(5)

            s1.metric("Capture Score", clean_value(row.get("early_capture_score")))
            s2.metric("Stage", clean_value(row.get("capture_stage")))
            s3.metric("MW Demand", clean_value(row.get("estimated_power_mw")))
            s4.metric("Relationships", len(project_relationships))
            s5.metric("Predictive", clean_bool(row.get("predictive_signal")))

            st.markdown("## Live Intelligence Feed")

            feed_items = []

            if clean_bool(row.get("predictive_signal")) == "Yes":
                feed_items.append("Predictive infrastructure signal identified")

            if clean_value(row.get("estimated_power_mw")) != "N/A":
                feed_items.append(f"Estimated MW demand detected: {clean_value(row.get('estimated_power_mw'))}")

            if clean_value(row.get("utility_dependency")) != "N/A":
                feed_items.append(f"Utility dependency mapped: {clean_value(row.get('utility_dependency'))}")

            if clean_value(row.get("risk_flags")) != "N/A":
                feed_items.append(f"Infrastructure risks identified: {clean_value(row.get('risk_flags'))}")

            feed_items.append(f"Project currently in {clean_value(row.get('capture_stage'))} stage")

            for item in feed_items:
                st.info(item)

            overview_tab, strategy_tab, relationship_tab, permit_tab, raw_tab = st.tabs(
                [
                    "Overview",
                    "Strategic Assessment",
                    "Relationships",
                    "Permit Intelligence",
                    "Raw Intelligence",
                ]
            )

            with overview_tab:
                c1, c2 = st.columns(2)

                with c1:
                    st.markdown(f"**Infrastructure Type:** {clean_value(row.get('infrastructure_type'))}")
                    st.markdown(f"**Project Stage:** {clean_value(row.get('project_stage'))}")
                    st.markdown(f"**County:** {clean_value(row.get('county'))}")
                    st.markdown(f"**Corridor Region:** {clean_value(row.get('corridor_region'))}")

                with c2:
                    st.markdown(f"**Market Cluster:** {clean_value(row.get('market_cluster'))}")
                    st.markdown(f"**Utility Provider:** {clean_value(row.get('utility_dependency'))}")
                    st.markdown(f"**Case Number:** {clean_value(row.get('case_number'))}")
                    st.markdown(f"**Created:** {clean_value(row.get('created_at'))}")

            with strategy_tab:
                st.markdown("## Executive Strategic Assessment")
                st.info(clean_value(row.get("strategic_notes"), "No strategic assessment generated yet."))

                st.markdown("## Infrastructure Risk Flags")
                risk_flags = clean_value(row.get("risk_flags"), "")

                if risk_flags == "":
                    st.success("No critical infrastructure risks detected.")
                else:
                    for risk in str(risk_flags).split(","):
                        if risk.strip():
                            st.warning(risk.strip())

            with relationship_tab:
                st.markdown("## Executive Relationship Intelligence")

                if project_relationships.empty:
                    st.warning("No executive relationships identified.")
                else:
                    st.success(f"{len(project_relationships)} executive relationships identified")

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

            with permit_tab:
                st.markdown("## Permit Intelligence")
                st.markdown(f"**Applicant:** {clean_value(row.get('applicant_name'))}")
                st.markdown(f"**Source Name:** {clean_value(row.get('source_name'))}")
                st.markdown(f"**Source Type:** {clean_value(row.get('source_type'))}")
                st.markdown(f"**Filing Date:** {clean_value(row.get('filing_date'))}")

                st.markdown("### Permit Description")
                st.write(clean_value(row.get("permit_description"), "No permit description available."))

            with raw_tab:
                st.markdown("## Raw Filing Intelligence")
                raw_text = clean_value(row.get("raw_text"), "No raw filing intelligence available.")
                st.code(raw_text[:10000])


st.markdown("## Executive Relationship Command Center")

relationship_search = st.text_input("Search Executive Relationships", key="relationship_search")

display_relationships = relationships_df.copy()

if relationship_search and not display_relationships.empty:
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
    "linkedin_url",
]

existing_relationship_cols = [
    c for c in relationship_columns
    if c in display_relationships.columns
]

if display_relationships.empty:
    st.info("No executive relationships found.")
else:
    st.dataframe(
        display_relationships[existing_relationship_cols],
        use_container_width=True,
        height=500,
    )

st.markdown("## Export Intelligence")

if not filtered_df.empty:
    st.download_button(
        "Download Infrastructure Intelligence CSV",
        filtered_df.to_csv(index=False),
        "infrastructure_intelligence.csv",
        "text/csv",
        key="download_infrastructure_csv",
    )

if not relationships_df.empty:
    st.download_button(
        "Download Executive Relationship Pipeline CSV",
        relationships_df.to_csv(index=False),
        "executive_relationship_pipeline.csv",
        "text/csv",
        key="download_relationship_csv",
    )

if st.button("Sign Out", key="sign_out_button"):
    st.session_state.user = None
    st.rerun()

st.caption("Allen Hammett AI • Institutional Infrastructure Intelligence Operating System")
