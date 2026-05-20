import os
import pandas as pd
import streamlit as st
import psycopg2
import pydeck as pdk
import plotly.express as px
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Allen Hammett AI",
    page_icon="🦁",
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


def safe_number(value, default=0):
    try:
        number = pd.to_numeric(value, errors="coerce")
        if pd.isna(number):
            return default
        return float(number)
    except Exception:
        return default


def clean_bool(value):
    if value in [True, "true", "True", "TRUE", 1, "1"]:
        return "Yes"
    if value in [False, "false", "False", "FALSE", 0, "0"]:
        return "No"
    return "Unknown"


def capture_stage(score):
    score = safe_number(score, 0)
    if score >= 90:
        return "Prime Positioning"
    if score >= 75:
        return "Strategic Development"
    if score >= 50:
        return "Active Monitoring"
    if score >= 25:
        return "Early Identification"
    return "Historical Context"


def signal_color(score):
    score = safe_number(score, 0)
    if score >= 90:
        return [0, 255, 170, 230]
    if score >= 75:
        return [0, 120, 255, 230]
    if score >= 50:
        return [255, 185, 40, 220]
    if score >= 25:
        return [150, 150, 150, 190]
    return [80, 80, 80, 160]


def signal_radius(score, mw, relationships):
    score = safe_number(score, 0)
    mw = safe_number(mw, 0)
    relationships = safe_number(relationships, 0)

    base = 900
    score_boost = score * 12
    mw_boost = min(mw, 500) * 3
    relationship_boost = min(relationships, 25) * 80

    radius = base + score_boost + mw_boost + relationship_boost

    if pd.isna(radius) or radius <= 0:
        return 1200

    return int(radius)


def influence_score(title):
    title = str(title or "").lower()
    score = 0
    if "chief" in title or "ceo" in title or "president" in title:
        score += 40
    if "vice president" in title or "vp" in title:
        score += 30
    if "director" in title:
        score += 25
    if "manager" in title:
        score += 15
    if "operations" in title:
        score += 20
    if "construction" in title:
        score += 20
    if "engineering" in title:
        score += 20
    if "public sector" in title:
        score += 25
    if "sales" in title or "business development" in title:
        score += 15
    return score


def recommended_actions(row, relationships_count):
    actions = []
    score = safe_number(row.get("early_capture_score"), 0)
    infrastructure_type = str(row.get("infrastructure_type") or "").lower()
    utility_dependency = clean_value(row.get("utility_dependency"), "")
    project_stage = str(row.get("project_stage") or "").lower()
    estimated_mw = clean_value(row.get("estimated_power_mw"), "")

    if score >= 90:
        actions.append("Escalate to immediate BD positioning review.")
    elif score >= 75:
        actions.append("Track as strategic development opportunity.")

    if "data center" in infrastructure_type:
        actions.append("Assess hyperscale ecosystem: utility, fiber, security, compliance, and construction stakeholders.")

    if utility_dependency != "":
        actions.append(f"Map utility relationship path connected to {utility_dependency}.")

    if estimated_mw not in ["", "N/A", "Unknown"]:
        actions.append("Evaluate power availability, transmission risk, and substation proximity.")

    if relationships_count > 0:
        actions.append("Prioritize outreach to matched executive contacts.")

    if "review" in project_stage or "submitted" in project_stage:
        actions.append("Monitor permit approval timeline and county planning updates.")

    if not actions:
        actions.append("Maintain monitoring status until stronger infrastructure signals appear.")

    return actions


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
        return False, "Invalid reset key."
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
    return True, "Password updated successfully."


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

    df["early_capture_score"] = pd.to_numeric(df["early_capture_score"], errors="coerce").fillna(0)
    df["capture_stage"] = df["early_capture_score"].apply(capture_stage)

    if "estimated_power_mw" in df.columns:
        df["estimated_power_mw"] = pd.to_numeric(df["estimated_power_mw"], errors="coerce")
    else:
        df["estimated_power_mw"] = 0

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    return df


@st.cache_data(ttl=300)
def load_relationships():
    try:
        df = run_query(
            """
            select *
            from executive_project_matches
            order by early_capture_score desc nulls last
            """
        )
        if not df.empty and "title" in df.columns:
            df["influence_score"] = df["title"].apply(influence_score)
        return df
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

st.sidebar.markdown("---")
st.sidebar.subheader("Alert Thresholds")
alert_score_threshold = st.sidebar.slider("Score Threshold", 50, 130, 90)
alert_mw_threshold = st.sidebar.slider("MW Threshold", 50, 500, 250)
alert_company_keyword = st.sidebar.text_input("Company / Utility Keyword")

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


st.markdown(
    """
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1500px;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.65rem;
        font-weight: 700;
    }
    div[data-testid="stMetricLabel"] {
        font-size: .82rem;
        color: #7f8b99;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 18px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 38px;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Infrastructure Intelligence Operating System")
st.caption("Allen Hammett AI — Executive Infrastructure Intelligence + Relationship Capture + Operational Awareness")

main_tab, operations_tab, relationships_tab, analytics_tab, exports_tab = st.tabs(
    [
        "Command Wall",
        "Opportunity Operations",
        "Relationship Command",
        "Market Analytics",
        "Exports",
    ]
)


with main_tab:
    k1, k2, k3, k4, k5, k6 = st.columns(6)

    k1.metric("Signals", len(filtered_df))
    k2.metric("Prime", len(filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]) if not filtered_df.empty else 0)
    k3.metric("Predictive", len(filtered_df[filtered_df["predictive_signal"] == True]) if not filtered_df.empty and "predictive_signal" in filtered_df.columns else 0)
    k4.metric("Relationships", len(relationships_df))
    k5.metric("Watchlist", len(st.session_state.watchlist))
    k6.metric("Mapped", len(filtered_df.dropna(subset=["latitude", "longitude"])) if not filtered_df.empty and {"latitude", "longitude"}.issubset(filtered_df.columns) else 0)

    st.markdown("## Infrastructure Intelligence Map")

    map_df = filtered_df.copy()

    if not map_df.empty and {"latitude", "longitude"}.issubset(map_df.columns):
        map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
        map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
        map_df = map_df.dropna(subset=["latitude", "longitude"]).copy()

        if not map_df.empty:
            if not relationships_df.empty and "canonical_project_name" in relationships_df.columns:
                rel_counts = relationships_df.groupby("canonical_project_name").size().reset_index(name="relationship_count")
                map_df = map_df.merge(rel_counts, on="canonical_project_name", how="left")
            else:
                map_df["relationship_count"] = 0

            map_df["relationship_count"] = pd.to_numeric(map_df["relationship_count"], errors="coerce").fillna(0).astype(int)
            map_df["estimated_power_mw"] = pd.to_numeric(map_df.get("estimated_power_mw", 0), errors="coerce").fillna(0)
            map_df["early_capture_score"] = pd.to_numeric(map_df.get("early_capture_score", 0), errors="coerce").fillna(0)

            map_df["color"] = map_df["early_capture_score"].apply(signal_color)
            map_df["radius"] = map_df.apply(
                lambda r: signal_radius(
                    r.get("early_capture_score", 0),
                    r.get("estimated_power_mw", 0),
                    r.get("relationship_count", 0),
                ),
                axis=1,
            )

            view_state = pdk.ViewState(
                latitude=float(map_df["latitude"].median()),
                longitude=float(map_df["longitude"].median()),
                zoom=8.6,
                pitch=42,
                bearing=0,
            )

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_df,
                get_position="[longitude, latitude]",
                get_fill_color="color",
                get_radius="radius",
                pickable=True,
                auto_highlight=True,
                opacity=0.82,
            )

            tooltip = {
                "html": """
                <div style="font-family:Arial; font-size:13px;">
                    <b>{canonical_project_name}</b><br/>
                    Score: {early_capture_score}<br/>
                    Stage: {capture_stage}<br/>
                    Type: {infrastructure_type}<br/>
                    MW: {estimated_power_mw}<br/>
                    Relationships: {relationship_count}<br/>
                    County: {county}<br/>
                    Market: {market_cluster}
                </div>
                """,
                "style": {
                    "backgroundColor": "#05070A",
                    "color": "#F5F7FA",
                    "border": "1px solid #1E5EFF",
                },
            }

            st.pydeck_chart(
                pdk.Deck(
                    map_style="mapbox://styles/mapbox/dark-v10",
                    initial_view_state=view_state,
                    layers=[layer],
                    tooltip=tooltip,
                ),
                use_container_width=True,
            )
        else:
            st.warning("No mapped infrastructure records found.")
    else:
        st.warning("Latitude / longitude fields are unavailable.")

    st.markdown("## Executive Signal Strip")

    signal_col1, signal_col2 = st.columns([1.3, 1])

    with signal_col1:
        alerts = []

        if not filtered_df.empty:
            high_score_df = filtered_df[filtered_df["early_capture_score"] >= alert_score_threshold]
            for _, row in high_score_df.head(4).iterrows():
                alerts.append(f"High-priority signal: {clean_value(row.get('canonical_project_name'))} scored {clean_value(row.get('early_capture_score'))}")

            if "estimated_power_mw" in filtered_df.columns:
                mw_df = filtered_df[pd.to_numeric(filtered_df["estimated_power_mw"], errors="coerce") >= alert_mw_threshold]
                for _, row in mw_df.head(4).iterrows():
                    alerts.append(f"Power demand signal: {clean_value(row.get('canonical_project_name'))} estimated at {clean_value(row.get('estimated_power_mw'))} MW")

            if alert_company_keyword:
                keyword_df = filtered_df[
                    filtered_df.astype(str).apply(
                        lambda row: row.str.contains(alert_company_keyword, case=False, na=False).any(),
                        axis=1,
                    )
                ]
                for _, row in keyword_df.head(4).iterrows():
                    alerts.append(f"Keyword signal '{alert_company_keyword}': {clean_value(row.get('canonical_project_name'))}")

        if alerts:
            for alert in alerts[:6]:
                st.info(alert)
        else:
            st.success("No critical threshold alerts under current settings.")

    with signal_col2:
        st.markdown("### Top Priority Queue")
        priority_cols = ["canonical_project_name", "early_capture_score", "capture_stage"]
        available_priority_cols = [c for c in priority_cols if c in filtered_df.columns]

        if not filtered_df.empty:
            st.dataframe(filtered_df[available_priority_cols].head(10), use_container_width=True, height=300)
        else:
            st.info("No priority records available.")


with operations_tab:
    left_panel, right_panel = st.columns([1.15, 2.85])

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

        queue_limit = st.slider("Visible Queue Size", min_value=10, max_value=200, value=40, key="visible_queue_size")
        queue_df = queue_df.head(queue_limit)

        if queue_df.empty:
            st.info("No projects match current filters.")
        else:
            for idx, project in queue_df.reset_index(drop=True).iterrows():
                project_name = clean_value(project.get("canonical_project_name"), "Unnamed Project")
                score = clean_value(project.get("early_capture_score"), "0")
                project_id = clean_value(project.get("id"), f"row_{idx}")

                if st.button(f"{project_name} | {score}", use_container_width=True, key=f"project_select_{project_id}_{idx}"):
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
                    if st.button(watch_item, key=f"watch_select_{idx}_{watch_item}", use_container_width=True):
                        st.session_state.selected_project = watch_item
                        st.rerun()
                with watch_col2:
                    if st.button("❌", key=f"watch_remove_{idx}_{watch_item}"):
                        st.session_state.watchlist.remove(watch_item)
                        st.rerun()

    with right_panel:
        selected_project = st.session_state.selected_project

        if not selected_project:
            st.info("Select a project from the Priority Infrastructure Queue.")
        else:
            selected_df = filtered_df[filtered_df["canonical_project_name"].astype(str) == str(selected_project)]

            if selected_df.empty:
                st.warning("Selected project is not available under current filters.")
            else:
                row = selected_df.iloc[0]
                project_relationships = pd.DataFrame()

                if not relationships_df.empty and "canonical_project_name" in relationships_df.columns:
                    project_relationships = relationships_df[
                        relationships_df["canonical_project_name"].astype(str) == str(selected_project)
                    ].copy()

                    if not project_relationships.empty and "influence_score" in project_relationships.columns:
                        project_relationships = project_relationships.sort_values("influence_score", ascending=False)

                st.markdown(f"# {selected_project}")

                s1, s2, s3, s4, s5 = st.columns(5)
                s1.metric("Score", clean_value(row.get("early_capture_score")))
                s2.metric("Stage", clean_value(row.get("capture_stage")))
                s3.metric("MW", clean_value(row.get("estimated_power_mw")))
                s4.metric("Relationships", len(project_relationships))
                s5.metric("Predictive", clean_bool(row.get("predictive_signal")))

                st.markdown("## AI Recommended Actions")
                for action in recommended_actions(row, len(project_relationships)):
                    st.success(action)

                overview_tab, strategy_tab, relationship_tab, permit_tab, raw_tab = st.tabs(["Overview", "Strategy", "Relationships", "Permit", "Raw Intel"])

                with overview_tab:
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown(f"**Infrastructure Type:** {clean_value(row.get('infrastructure_type'))}")
                        st.markdown(f"**Project Stage:** {clean_value(row.get('project_stage'))}")
                        st.markdown(f"**County:** {clean_value(row.get('county'))}")
                        st.markdown(f"**Corridor:** {clean_value(row.get('corridor_region'))}")
                    with c2:
                        st.markdown(f"**Market Cluster:** {clean_value(row.get('market_cluster'))}")
                        st.markdown(f"**Utility Provider:** {clean_value(row.get('utility_dependency'))}")
                        st.markdown(f"**Case Number:** {clean_value(row.get('case_number'))}")
                        st.markdown(f"**Created:** {clean_value(row.get('created_at'))}")

                with strategy_tab:
                    st.markdown("### Executive Strategic Assessment")
                    st.info(clean_value(row.get("strategic_notes"), "No strategic assessment generated yet."))

                    st.markdown("### Infrastructure Risk Flags")
                    risk_flags = clean_value(row.get("risk_flags"), "")
                    if risk_flags == "":
                        st.success("No critical infrastructure risks detected.")
                    else:
                        for risk in str(risk_flags).split(","):
                            if risk.strip():
                                st.warning(risk.strip())

                with relationship_tab:
                    st.markdown("### Executive Relationship Intelligence")
                    if project_relationships.empty:
                        st.warning("No executive relationships identified.")
                    else:
                        st.success(f"{len(project_relationships)} executive relationships identified")
                        relationship_columns = ["full_name", "title", "company", "email", "linkedin_url", "influence_score"]
                        existing_cols = [c for c in relationship_columns if c in project_relationships.columns]
                        st.dataframe(project_relationships[existing_cols], use_container_width=True, height=450)

                with permit_tab:
                    st.markdown("### Permit Intelligence")
                    st.markdown(f"**Applicant:** {clean_value(row.get('applicant_name'))}")
                    st.markdown(f"**Source Name:** {clean_value(row.get('source_name'))}")
                    st.markdown(f"**Source Type:** {clean_value(row.get('source_type'))}")
                    st.markdown(f"**Filing Date:** {clean_value(row.get('filing_date'))}")
                    st.markdown("### Permit Description")
                    st.write(clean_value(row.get("permit_description"), "No permit description available."))

                with raw_tab:
                    st.markdown("### Raw Filing Intelligence")
                    raw_text = clean_value(row.get("raw_text"), "No raw filing intelligence available.")
                    st.code(raw_text[:10000])


with relationships_tab:
    st.markdown("## Executive Relationship Command Center")
    relationship_search = st.text_input("Search Executive Relationships", key="relationship_search")

    display_relationships = relationships_df.copy()

    if relationship_search and not display_relationships.empty:
        display_relationships = display_relationships[
            display_relationships.astype(str).apply(
                lambda row: row.str.contains(relationship_search, case=False, na=False).any(),
                axis=1,
            )
        ]

    relationship_columns = ["canonical_project_name", "company", "full_name", "title", "email", "linkedin_url", "influence_score"]
    existing_relationship_cols = [c for c in relationship_columns if c in display_relationships.columns]

    if display_relationships.empty:
        st.info("No executive relationships found.")
    else:
        st.dataframe(display_relationships[existing_relationship_cols], use_container_width=True, height=650)


with analytics_tab:
    st.markdown("## Market Analytics")

    a1, a2 = st.columns(2)

    with a1:
        st.markdown("### Capture Stage Distribution")
        if not filtered_df.empty and "capture_stage" in filtered_df.columns:
            stage_chart = filtered_df["capture_stage"].value_counts().reset_index()
            stage_chart.columns = ["Capture Stage", "Count"]
            fig = px.bar(stage_chart, x="Capture Stage", y="Count", template="plotly_dark", color="Capture Stage")
            fig.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with a2:
        st.markdown("### Infrastructure Type Mix")
        if not filtered_df.empty and "infrastructure_type" in filtered_df.columns:
            infra_chart = filtered_df["infrastructure_type"].fillna("Unknown").value_counts().head(10).reset_index()
            infra_chart.columns = ["Infrastructure Type", "Count"]
            fig = px.bar(infra_chart, x="Infrastructure Type", y="Count", template="plotly_dark", color="Count")
            fig.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    b1, b2 = st.columns(2)

    with b1:
        st.markdown("### Signals Over Time")
        if not filtered_df.empty and "created_at" in filtered_df.columns:
            trend_df = filtered_df.dropna(subset=["created_at"]).copy()
            if not trend_df.empty:
                trend_df["date"] = trend_df["created_at"].dt.date
                trend_chart = trend_df.groupby("date").size().reset_index(name="Signals")
                fig = px.line(trend_chart, x="date", y="Signals", template="plotly_dark", markers=True)
                fig.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)

    with b2:
        st.markdown("### Relationship Influence Leaders")
        if not relationships_df.empty and "influence_score" in relationships_df.columns:
            leader_cols = ["full_name", "title", "company", "influence_score"]
            available_leader_cols = [c for c in leader_cols if c in relationships_df.columns]
            st.dataframe(relationships_df[available_leader_cols].sort_values("influence_score", ascending=False).head(15), use_container_width=True, height=360)


with exports_tab:
    st.markdown("## Export Intelligence")

    if not filtered_df.empty:
        st.download_button("Download Infrastructure Intelligence CSV", filtered_df.to_csv(index=False), "infrastructure_intelligence.csv", "text/csv", key="download_infrastructure_csv")

    if not relationships_df.empty:
        st.download_button("Download Executive Relationship Pipeline CSV", relationships_df.to_csv(index=False), "executive_relationship_pipeline.csv", "text/csv", key="download_relationship_csv")

    if st.button("Sign Out", key="sign_out_button"):
        st.session_state.user = None
        st.rerun()


st.caption("Allen Hammett AI • Institutional Infrastructure Intelligence Operating System")
