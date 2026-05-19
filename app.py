import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

# =========================================================
# PAGE CONFIG
# =========================================================

st.set_page_config(
    page_title="Allen Hammett AI",
    page_icon="🏗️",
    layout="wide"
)

# =========================================================
# DATABASE
# =========================================================

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

# =========================================================
# QUERY FUNCTION
# =========================================================

@st.cache_data(ttl=3600)
def run_query(query):
    return pd.read_sql(query, engine)

# =========================================================
# LOAD DATA
# =========================================================

projects_df = run_query("""
SELECT *
FROM projects
WHERE early_capture_score >= 75
ORDER BY early_capture_score DESC
LIMIT 250
""")

relationships_df = run_query("""
SELECT *
FROM executive_project_matches
LIMIT 1000
""")

# =========================================================
# SIDEBAR
# =========================================================

st.sidebar.title("Executive Command Filters")

search_term = st.sidebar.text_input(
    "Global Intelligence Search"
)

prime_only = st.sidebar.checkbox("Prime Positioning Only")

predictive_only = st.sidebar.checkbox("Predictive Signals Only")

watchlist_only = st.sidebar.checkbox("Watchlist Only")

# =========================================================
# FILTERS
# =========================================================

filtered_projects = projects_df.copy()

if search_term:
    filtered_projects = filtered_projects[
        filtered_projects["canonical_project_name"]
        .str.contains(search_term, case=False, na=False)
    ]

if prime_only:
    filtered_projects = filtered_projects[
        filtered_projects["early_capture_score"] >= 90
    ]

if predictive_only:
    filtered_projects = filtered_projects[
        filtered_projects["predictive_signal"] == True
    ]

# =========================================================
# HEADER
# =========================================================

st.title("Allen Hammett AI")

st.caption(
    "Executive Infrastructure Intelligence + Relationship Capture Platform"
)

# =========================================================
# EXECUTIVE KPI ROW
# =========================================================

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        "Signals",
        len(projects_df)
    )

with col2:
    st.metric(
        "Prime",
        len(projects_df[projects_df["early_capture_score"] >= 90])
    )

with col3:
    st.metric(
        "Predictive",
        len(projects_df[
            projects_df["predictive_signal"] == True
        ])
    )

with col4:
    st.metric(
        "Relationships",
        len(relationships_df)
    )

with col5:
    st.metric(
        "Mapped Companies",
        relationships_df["company"].nunique()
    )

st.divider()

# =========================================================
# MAIN LAYOUT
# =========================================================

left_col, right_col = st.columns([1.1, 1.4])

# =========================================================
# LEFT COLUMN
# =========================================================

with left_col:

    st.subheader("Priority Infrastructure Queue")

    queue_size = st.slider(
        "Visible Queue Size",
        5,
        50,
        15
    )

    visible_projects = filtered_projects.head(queue_size)

    # -----------------------------------------------------

    project_names = visible_projects[
        "canonical_project_name"
    ].tolist()

    selected_project_name = st.radio(
        "Select Active Project",
        project_names,
        label_visibility="collapsed"
    )

# =========================================================
# SELECT PROJECT
# =========================================================

selected_project = filtered_projects[
    filtered_projects["canonical_project_name"]
    == selected_project_name
].iloc[0]

# =========================================================
# RIGHT COLUMN
# =========================================================

with right_col:

    st.subheader(selected_project["canonical_project_name"])

    detail_col1, detail_col2 = st.columns(2)

    with detail_col1:

        st.markdown(
            f"""
            **Capture Stage:** {selected_project.get('capture_stage', 'N/A')}

            **Capture Score:** {selected_project.get('early_capture_score', 'N/A')}

            **Infrastructure Type:** {selected_project.get('infrastructure_type', 'N/A')}

            **Project Stage:** {selected_project.get('project_stage', 'N/A')}

            **Case Number:** {selected_project.get('case_number', 'N/A')}
            """
        )

    with detail_col2:

        st.markdown(
            f"""
            **County:** {selected_project.get('county', 'N/A')}

            **Market Cluster:** {selected_project.get('market_cluster', 'N/A')}

            **Estimated MW Demand:** {selected_project.get('estimated_power_mw', 'N/A')}

            **Predictive Signal:** {"Yes" if selected_project.get('predictive_signal') else "No"}

            **Utility Provider:** {selected_project.get('utility_provider', 'N/A')}
            """
        )

    st.divider()

    # =====================================================
    # TABS
    # =====================================================

    tab1, tab2, tab3, tab4 = st.tabs([
        "Strategic Assessment",
        "Relationship Intelligence",
        "Permit Intelligence",
        "Market Analytics"
    ])

    # =====================================================
    # TAB 1
    # =====================================================

    with tab1:

        st.subheader("Executive Strategic Assessment")

        strategic_notes = selected_project.get(
            "strategic_notes",
            "No strategic assessment available."
        )

        st.info(strategic_notes)

        risk_flags = selected_project.get(
            "risk_flags",
            "No risk flags identified."
        )

        st.warning(risk_flags)

    # =====================================================
    # TAB 2
    # =====================================================

    with tab2:

        st.subheader("Relationship Intelligence")

        project_relationships = relationships_df[
            relationships_df["canonical_project_name"]
            == selected_project_name
        ]

        if len(project_relationships) > 0:

            st.success(
                f"{len(project_relationships)} executive relationships mapped"
            )

            st.dataframe(
                project_relationships[
                    [
                        "full_name",
                        "title",
                        "company",
                        "email"
                    ]
                ],
                use_container_width=True,
                height=500
            )

        else:

            st.warning(
                "No executive relationships mapped yet."
            )

    # =====================================================
    # TAB 3
    # =====================================================

    with tab3:

        st.subheader("Permit Intelligence")

        st.code(
            selected_project.get(
                "raw_filing_text",
                "No raw filing intelligence available."
            )
        )

    # =====================================================
    # TAB 4
    # =====================================================

    with tab4:

        st.subheader("Market Analytics")

        analytics_col1, analytics_col2 = st.columns(2)

        with analytics_col1:

            stage_chart = px.pie(
                filtered_projects,
                names="project_stage",
                title="Project Stage Distribution"
            )

            st.plotly_chart(
                stage_chart,
                use_container_width=True
            )

        with analytics_col2:

            type_chart = px.bar(
                filtered_projects.groupby(
                    "infrastructure_type"
                ).size().reset_index(name="count"),
                x="infrastructure_type",
                y="count",
                title="Infrastructure Types"
            )

            st.plotly_chart(
                type_chart,
                use_container_width=True
            )

# =========================================================
# MAP
# =========================================================

st.divider()

st.subheader("Infrastructure Intelligence Map")

map_df = filtered_projects.dropna(
    subset=["latitude", "longitude"]
)

if len(map_df) > 0:

    st.map(
        map_df.rename(columns={
            "latitude": "lat",
            "longitude": "lon"
        }),
        size=15
    )

# =========================================================
# LIVE FEED
# =========================================================

st.divider()

st.subheader("Executive Signal Feed")

feed_df = filtered_projects.head(10)

for _, row in feed_df.iterrows():

    st.info(
        f"""
        {row['canonical_project_name']}

        Score: {row['early_capture_score']} |
        Stage: {row['project_stage']} |
        Market: {row['market_cluster']}
        """
    )

# =========================================================
# FOOTER
# =========================================================

st.caption(
    "Allen Hammett AI • Executive Infrastructure Intelligence Platform"
)
