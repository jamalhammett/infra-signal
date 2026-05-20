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
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATABASE_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))

# =========================================================
# DATABASE
# =========================================================

def run_query(sql, params=None):
    conn = psycopg2.connect(DATABASE_URL)
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


# =========================================================
# HELPERS
# =========================================================

def safe_number(v, default=0):
    try:
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x):
            return default
        return float(x)
    except:
        return default


def clean(v, fallback="N/A"):
    try:
        if pd.isna(v):
            return fallback
    except:
        pass

    if v is None:
        return fallback

    txt = str(v).strip()

    if txt.lower() in ["nan", "none", "null", ""]:
        return fallback

    return txt


def capture_stage(score):
    score = safe_number(score)

    if score >= 90:
        return "Prime Positioning"

    if score >= 75:
        return "Strategic Development"

    if score >= 50:
        return "Active Monitoring"

    if score >= 25:
        return "Early Identification"

    return "Historical Context"


def influence_score(title):
    title = str(title).lower()

    score = 0

    if "chief" in title:
        score += 50

    if "ceo" in title:
        score += 50

    if "president" in title:
        score += 40

    if "vice president" in title or "vp" in title:
        score += 35

    if "director" in title:
        score += 25

    if "operations" in title:
        score += 20

    if "construction" in title:
        score += 20

    if "engineering" in title:
        score += 18

    if "public sector" in title:
        score += 15

    if "strategic" in title:
        score += 10

    return score


def relationship_tier(score):
    if score >= 60:
        return "HIGH"

    if score >= 35:
        return "MEDIUM"

    return "LOW"


def threat_level(score, mw, relationships):
    mw = safe_number(mw)
    score = safe_number(score)
    relationships = safe_number(relationships)

    risk = 0

    if mw >= 250:
        risk += 40

    if score >= 90:
        risk += 35

    if relationships <= 2:
        risk += 25

    if risk >= 70:
        return "CRITICAL"

    if risk >= 45:
        return "ELEVATED"

    return "MONITOR"


def recommendation(project_row, relationship_count):

    score = safe_number(project_row.get("early_capture_score"))
    mw = safe_number(project_row.get("estimated_power_mw"))

    stage = str(project_row.get("project_stage")).lower()

    if score >= 90 and relationship_count <= 2:
        return "URGENT: Expand executive relationship coverage immediately."

    if mw >= 250:
        return "Initiate executive utility and operations outreach."

    if "review" in stage:
        return "Monitor permitting cycle and procurement timing."

    return "Maintain strategic monitoring posture."


def signal_color(score):

    score = safe_number(score)

    if score >= 90:
        return [0, 255, 170, 220]

    if score >= 75:
        return [0, 140, 255, 220]

    if score >= 50:
        return [255, 185, 40, 210]

    return [140, 140, 140, 180]


def signal_radius(score):

    score = safe_number(score)

    return int(1200 + (score * 25))


# =========================================================
# LOAD DATA
# =========================================================

@st.cache_data(ttl=300)
def load_projects():

    df = run_query("""
        SELECT *
        FROM projects
        ORDER BY early_capture_score DESC NULLS LAST
        LIMIT 5000
    """)

    if df.empty:
        return df

    df["early_capture_score"] = pd.to_numeric(
        df["early_capture_score"],
        errors="coerce"
    ).fillna(0)

    df["capture_stage"] = df["early_capture_score"].apply(capture_stage)

    return df


@st.cache_data(ttl=300)
def load_relationships():

    try:

        df = run_query("""
            SELECT *
            FROM executive_project_matches
        """)

        if not df.empty:
            df["influence_score"] = df["title"].apply(influence_score)
            df["relationship_tier"] = df["influence_score"].apply(
                relationship_tier
            )

        return df

    except:
        return pd.DataFrame()


projects_df = load_projects()
relationships_df = load_relationships()

# =========================================================
# SESSION STATE
# =========================================================

if "selected_project" not in st.session_state:

    if not projects_df.empty:
        st.session_state.selected_project = projects_df.iloc[0][
            "canonical_project_name"
        ]
    else:
        st.session_state.selected_project = None

# =========================================================
# STYLING
# =========================================================

st.markdown("""
<style>

.block-container{
    padding-top:1rem;
    max-width:1600px;
}

html, body, [class*="css"]{
    font-family:Inter,sans-serif;
}

div[data-testid="stMetricValue"]{
    font-size:1.7rem;
    font-weight:700;
}

div[data-testid="stMetricLabel"]{
    color:#7c8a9a;
}

.stTabs [data-baseweb="tab"]{
    font-weight:600;
}

</style>
""", unsafe_allow_html=True)

# =========================================================
# HEADER
# =========================================================

st.title("Infrastructure Intelligence Operating System")

st.caption(
    "Allen Hammett AI — Institutional Infrastructure Intelligence + Capture Operations"
)

# =========================================================
# TOP METRICS
# =========================================================

m1, m2, m3, m4, m5, m6 = st.columns(6)

m1.metric("Signals", len(projects_df))

m2.metric(
    "Prime",
    len(projects_df[projects_df["capture_stage"] == "Prime Positioning"])
)

m3.metric(
    "Predictive",
    len(projects_df[projects_df["capture_stage"] == "Strategic Development"])
)

m4.metric("Relationships", len(relationships_df))

m5.metric(
    "Mapped",
    len(
        projects_df.dropna(
            subset=["latitude", "longitude"]
        )
    )
)

m6.metric(
    "Watchlist",
    0
)

# =========================================================
# LAYOUT
# =========================================================

left, center, right = st.columns([1.15, 2.2, 1.2])

# =========================================================
# LEFT PANEL
# =========================================================

with left:

    st.markdown("## Priority Infrastructure Queue")

    search = st.text_input(
        "Quick Search",
        placeholder="Search infrastructure..."
    )

    queue_df = projects_df.copy()

    if search:

        queue_df = queue_df[
            queue_df["canonical_project_name"]
            .astype(str)
            .str.contains(search, case=False, na=False)
        ]

    queue_df = queue_df.sort_values(
        "early_capture_score",
        ascending=False
    )

    queue_df = queue_df.head(40)

    for idx, row in queue_df.iterrows():

        name = clean(row.get("canonical_project_name"))
        score = clean(row.get("early_capture_score"))

        if st.button(
            f"{name} | {score}",
            key=f"project_{idx}",
            use_container_width=True
        ):
            st.session_state.selected_project = name
            st.rerun()

# =========================================================
# CENTER PANEL
# =========================================================

with center:

    st.markdown("## Infrastructure Intelligence Map")

    map_df = projects_df.copy()

    map_df["latitude"] = pd.to_numeric(
        map_df["latitude"],
        errors="coerce"
    )

    map_df["longitude"] = pd.to_numeric(
        map_df["longitude"],
        errors="coerce"
    )

    map_df = map_df.dropna(
        subset=["latitude", "longitude"]
    )

    map_df["color"] = map_df[
        "early_capture_score"
    ].apply(signal_color)

    map_df["radius"] = map_df[
        "early_capture_score"
    ].apply(signal_radius)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[longitude, latitude]",
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
        opacity=0.85,
    )

    view_state = pdk.ViewState(
        latitude=39.02,
        longitude=-77.45,
        zoom=8.2,
        pitch=45,
    )

    deck = pdk.Deck(
        map_style=pdk.map_styles.CARTO_DARK,
        initial_view_state=view_state,
        layers=[layer],
        tooltip={
            "html": """
            <b>{canonical_project_name}</b><br/>
            Score: {early_capture_score}<br/>
            Stage: {capture_stage}<br/>
            Type: {infrastructure_type}
            """
        }
    )

    st.pydeck_chart(deck, use_container_width=True)

    st.markdown("## Executive Deal Control")

    selected_project = st.session_state.selected_project

    selected_df = projects_df[
        projects_df["canonical_project_name"]
        == selected_project
    ]

    if not selected_df.empty:

        row = selected_df.iloc[0]

        project_relationships = relationships_df[
            relationships_df["canonical_project_name"]
            == selected_project
        ].copy()

        relationship_count = len(project_relationships)

        score = safe_number(
            row.get("early_capture_score")
        )

        mw = safe_number(
            row.get("estimated_power_mw")
        )

        threat = threat_level(
            score,
            mw,
            relationship_count
        )

        rec = recommendation(
            row,
            relationship_count
        )

        s1, s2, s3, s4 = st.columns(4)

        s1.metric(
            "Capture Score",
            int(score)
        )

        s2.metric(
            "MW Opportunity",
            int(mw) if mw > 0 else "N/A"
        )

        s3.metric(
            "Relationships",
            relationship_count
        )

        s4.metric(
            "Threat Level",
            threat
        )

        st.markdown("---")

        st.markdown(f"### {selected_project}")

        c1, c2 = st.columns(2)

        with c1:

            st.markdown(
                f"**Capture Stage:** {clean(row.get('capture_stage'))}"
            )

            st.markdown(
                f"**Project Stage:** {clean(row.get('project_stage'))}"
            )

            st.markdown(
                f"**County:** {clean(row.get('county'))}"
            )

            st.markdown(
                f"**Market:** {clean(row.get('market_cluster'))}"
            )

        with c2:

            st.markdown(
                f"**Infrastructure Type:** {clean(row.get('infrastructure_type'))}"
            )

            st.markdown(
                f"**Utility:** {clean(row.get('utility_dependency'))}"
            )

            st.markdown(
                f"**Predictive Signal:** {clean(row.get('predictive_signal'))}"
            )

            st.markdown(
                f"**Case Number:** {clean(row.get('case_number'))}"
            )

        st.markdown("---")

        st.markdown("### AI Recommended Action")

        st.success(rec)

        st.markdown("---")

        st.markdown("### Threat Intelligence")

        if threat == "CRITICAL":

            st.error(
                "High-value infrastructure opportunity with insufficient relationship penetration."
            )

        elif threat == "ELEVATED":

            st.warning(
                "Strategic monitoring recommended due to opportunity scale."
            )

        else:

            st.info(
                "Infrastructure opportunity remains under active monitoring."
            )

# =========================================================
# RIGHT PANEL
# =========================================================

with right:

    st.markdown("## Executive Influence Grid")

    if not relationships_df.empty:

        rel_df = relationships_df[
            relationships_df["canonical_project_name"]
            == st.session_state.selected_project
        ].copy()

        rel_df = rel_df.sort_values(
            "influence_score",
            ascending=False
        )

        if rel_df.empty:

            st.info(
                "No executive relationships mapped."
            )

        else:

            for idx, row in rel_df.head(15).iterrows():

                tier = clean(
                    row.get("relationship_tier")
                )

                if tier == "HIGH":
                    st.error(
                        f"{clean(row.get('full_name'))}\n\n"
                        f"{clean(row.get('title'))}"
                    )

                elif tier == "MEDIUM":
                    st.warning(
                        f"{clean(row.get('full_name'))}\n\n"
                        f"{clean(row.get('title'))}"
                    )

                else:
                    st.info(
                        f"{clean(row.get('full_name'))}\n\n"
                        f"{clean(row.get('title'))}"
                    )

    st.markdown("---")

    st.markdown("## Market Analytics")

    stage_counts = (
        projects_df["capture_stage"]
        .value_counts()
        .reset_index()
    )

    stage_counts.columns = [
        "Stage",
        "Count"
    ]

    fig = px.bar(
        stage_counts,
        x="Stage",
        y="Count",
        template="plotly_dark",
        color="Count"
    )

    fig.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=20, b=10)
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

st.caption(
    "Allen Hammett AI • Institutional Capture Intelligence Operating System"
)
