import os
import pandas as pd
import psycopg2 as psycopg
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))

st.set_page_config(
    page_title="Infrastructure Intelligence Platform",
    layout="wide"
)

TARGET_KEYWORDS = [
    "data center", "datacenter", "hyperscale", "cloud", "server",
    "substation", "switchyard", "transmission", "utility",
    "power", "energy", "fiber", "telecom", "industrial"
]

EXCLUDED_KEYWORDS = [
    "sidewalk", "trail", "path", "driveway", "subdivision",
    "townhome", "townhomes", "single family", "residential",
    "lot ", "lots", "farm", "vineyard", "conservancy",
    "school", "church", "playground", "park", "landscape",
    "forest", "stormwater", "road widening", "firehouse",
    "barrister", "golden", "auto world"
]

DIRECT_KEYWORDS = [
    "data center", "datacenter", "hyperscale", "cloud", "server",
    "campus", "colo", "colocation"
]

UPSTREAM_KEYWORDS = [
    "substation", "switchyard", "transmission", "power",
    "energy", "utility", "novec", "dominion"
]

INDUSTRIAL_KEYWORDS = [
    "industrial", "warehouse", "logistics", "manufacturing"
]


def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
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

    email = st.text_input("Email")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        user = authenticate(email, password)

        if user:
            st.session_state.user = user
            st.rerun()
        else:
            st.error("Invalid credentials")

    st.stop()


def is_qualified_project(row):
    text = " ".join([str(v) for v in row.fillna("").values]).lower()

    if any(bad in text for bad in EXCLUDED_KEYWORDS):
        return False

    return any(good in text for good in TARGET_KEYWORDS)


def classify_category(row):
    text = " ".join([str(v) for v in row.fillna("").values]).lower()

    if any(k in text for k in DIRECT_KEYWORDS):
        return "Direct Opportunity"

    if any(k in text for k in UPSTREAM_KEYWORDS):
        return "Upstream Infrastructure Signal"

    if any(k in text for k in INDUSTRIAL_KEYWORDS):
        return "Industrial Infrastructure"

    return "Watchlist"


def opportunity_score(row):
    text = " ".join([str(v) for v in row.fillna("").values]).lower()
    score = 0

    if "data center" in text or "datacenter" in text:
        score += 45

    if any(k in text for k in ["cyrusone", "vantage", "qts", "digital realty", "stack", "equinix", "cologix", "coresite", "aligned"]):
        score += 30

    if any(k in text for k in ["substation", "transmission", "switchyard", "novec", "dominion"]):
        score += 25

    if any(k in text for k in ["approved", "in review", "submitted", "pending"]):
        score += 15

    if any(k in text for k in ["industrial", "warehouse", "fiber", "telecom"]):
        score += 10

    if any(bad in text for bad in EXCLUDED_KEYWORDS):
        score -= 100

    return max(score, 0)


def priority_label(score):
    if score >= 80:
        return "HIGH"
    if score >= 55:
        return "MEDIUM"
    if score >= 30:
        return "WATCHLIST"
    return "LOW"


def recommended_action(row):
    category = row.get("intelligence_category", "")
    score = row.get("opportunity_score", 0)

    if category == "Direct Opportunity" and score >= 80:
        return "Prioritize BD outreach and identify project delivery / procurement stakeholders."

    if category == "Direct Opportunity":
        return "Track for BD positioning and identify owner, developer, and contractor relationships."

    if category == "Upstream Infrastructure Signal":
        return "Monitor as early capture intelligence. Correlate with future data center, utility, and land development filings."

    if category == "Industrial Infrastructure":
        return "Evaluate for contractor, equipment, and supplier demand."

    return "Keep on watchlist until additional activity confirms value."


def get_projects():
    sql = """
    select
        case_number,
        canonical_project_name,
        project_stage,
        project_type,
        county,
        state,
        source_name,
        source_type,
        created_at
    from projects
    where created_at >= now() - interval '90 days'
    order by created_at desc
    limit 2000
    """

    df = run_query(sql)

    if df.empty:
        return df

    df = df[df.apply(is_qualified_project, axis=1)].copy()

    df["intelligence_category"] = df.apply(classify_category, axis=1)
    df["opportunity_score"] = df.apply(opportunity_score, axis=1)
    df["priority"] = df["opportunity_score"].apply(priority_label)
    df["recommended_action"] = df.apply(recommended_action, axis=1)

    df = df.sort_values(
        by=["opportunity_score", "created_at"],
        ascending=[False, False]
    )

    return df


def get_leads():
    sql = """
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

    return run_query(sql)


user = login_gate()

projects_df = get_projects()
leads_df = get_leads()
filtered_df = projects_df.copy()

st.sidebar.header("Infrastructure Intelligence Filters")

if not projects_df.empty:
    county_options = ["All"] + sorted(projects_df["county"].dropna().unique().tolist())
    state_options = ["All"] + sorted(projects_df["state"].dropna().unique().tolist())
    source_options = ["All"] + sorted(projects_df["source_name"].dropna().unique().tolist())
    stage_options = ["All"] + sorted(projects_df["project_stage"].dropna().unique().tolist())
    category_options = ["All"] + sorted(projects_df["intelligence_category"].dropna().unique().tolist())
    priority_options = ["All"] + sorted(projects_df["priority"].dropna().unique().tolist())

    county_filter = st.sidebar.selectbox("County", county_options)
    state_filter = st.sidebar.selectbox("State", state_options)
    source_filter = st.sidebar.selectbox("Source", source_options)
    stage_filter = st.sidebar.selectbox("Stage", stage_options)
    category_filter = st.sidebar.selectbox("Intelligence Category", category_options)
    priority_filter = st.sidebar.selectbox("Priority", priority_options)

    if county_filter != "All":
        filtered_df = filtered_df[filtered_df["county"] == county_filter]

    if state_filter != "All":
        filtered_df = filtered_df[filtered_df["state"] == state_filter]

    if source_filter != "All":
        filtered_df = filtered_df[filtered_df["source_name"] == source_filter]

    if stage_filter != "All":
        filtered_df = filtered_df[filtered_df["project_stage"] == stage_filter]

    if category_filter != "All":
        filtered_df = filtered_df[filtered_df["intelligence_category"] == category_filter]

    if priority_filter != "All":
        filtered_df = filtered_df[filtered_df["priority"] == priority_filter]


st.title("Infrastructure Intelligence Platform")
st.markdown("Allen Hammett AI — Executive Infrastructure / Data Center Intelligence")

high_count = len(filtered_df[filtered_df["priority"] == "HIGH"]) if not filtered_df.empty else 0
direct_count = len(filtered_df[filtered_df["intelligence_category"] == "Direct Opportunity"]) if not filtered_df.empty else 0
upstream_count = len(filtered_df[filtered_df["intelligence_category"] == "Upstream Infrastructure Signal"]) if not filtered_df.empty else 0

col1, col2, col3, col4 = st.columns(4)

col1.metric("Qualified Opportunities", len(filtered_df))
col2.metric("High Priority", high_count)
col3.metric("Direct Opportunities", direct_count)
col4.metric("Upstream Signals", upstream_count)

st.markdown("## Executive Priority Opportunities")

display_cols = [
    "priority",
    "opportunity_score",
    "intelligence_category",
    "case_number",
    "canonical_project_name",
    "project_stage",
    "project_type",
    "county",
    "state",
    "source_name",
    "recommended_action",
    "created_at",
]

existing_display_cols = [c for c in display_cols if c in filtered_df.columns]

st.dataframe(
    filtered_df[existing_display_cols],
    use_container_width=True
)

st.markdown("## Infrastructure Leads")

st.dataframe(
    leads_df,
    use_container_width=True
)

st.markdown("## Export Intelligence")

if not filtered_df.empty:
    project_csv = filtered_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download Opportunities CSV",
        data=project_csv,
        file_name="infrastructure_opportunities.csv",
        mime="text/csv"
    )

if not leads_df.empty:
    leads_csv = leads_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download Leads CSV",
        data=leads_csv,
        file_name="infrastructure_leads.csv",
        mime="text/csv"
    )

if st.button("Sign Out"):
    st.session_state.user = None
    st.rerun()

st.caption("Allen Hammett AI • Infrastructure Intelligence System")
