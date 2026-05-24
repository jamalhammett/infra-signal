import os
import re
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


def normalize_text(value):
    text = clean_value(value, "").lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def canonical_company(value):
    text = normalize_text(value)

    aliases = {
        "vantage": ["vantage", "vantage data centers", "vantage dc"],
        "qts": ["qts", "qts data centers", "quality technology services", "qtsdatacenters"],
        "stack": ["stack", "stack infrastructure", "stackinfra"],
        "digital realty": ["digital realty", "digitalrealty", "digital realty trust"],
        "equinix": ["equinix"],
        "cyrusone": ["cyrusone", "cyrus one"],
        "aligned": ["aligned", "aligned data centers", "aligneddc"],
        "coresite": ["coresite", "core site"],
        "cologix": ["cologix"],
        "dominion energy": ["dominion", "dominion energy", "dominionenergy", "dom"],
        "novec": ["novec", "northern virginia electric cooperative"],
        "dpr construction": ["dpr", "dpr construction"],
        "turner construction": ["turner", "turner construction"],
        "hitt": ["hitt", "hitt contracting"],
        "clayco": ["clayco"],
        "whiting turner": ["whiting turner", "whiting-turner"],
        "burns mcdonnell": ["burns mcdonnell", "burns and mcdonnell"],
        "jacobs": ["jacobs"],
        "hdr": ["hdr"],
        "black veatch": ["black veatch", "black and veatch"],
        "zayo": ["zayo"],
        "lumen": ["lumen", "lumen technologies"],
        "crown castle": ["crown castle"],
        "compass datacenters": ["compass", "compass datacenters", "compass data centers"],
        "ntt global data centers": ["ntt", "ntt global data centers"],
        "edgecore": ["edgecore", "edgecore digital infrastructure"],
        "databank": ["databank", "data bank"],
    }

    for canonical, keys in aliases.items():
        for key in keys:
            if key in text:
                return canonical

    return text


def extract_project_companies(row):
    candidate_fields = [
        "canonical_project_name",
        "project_name",
        "applicant_name",
        "owner",
        "developer",
        "company",
        "company_name",
        "utility_dependency",
        "utility_provider",
        "source_name",
        "permit_description",
        "raw_text",
        "strategic_notes",
    ]

    detected = set()

    for field in candidate_fields:
        if field in row.index:
            value = normalize_text(row.get(field))
            if value:
                detected.add(canonical_company(value))

    combined = " ".join([normalize_text(row.get(field)) for field in candidate_fields if field in row.index])

    known_companies = [
        "vantage", "qts", "stack", "digital realty", "equinix", "cyrusone",
        "aligned", "coresite", "cologix", "dominion energy", "novec",
        "dpr construction", "turner construction", "hitt", "clayco",
        "whiting turner", "burns mcdonnell", "jacobs", "hdr", "black veatch",
        "zayo", "lumen", "crown castle", "compass datacenters",
        "ntt global data centers", "edgecore", "databank",
    ]

    for company in known_companies:
        if canonical_company(company) in canonical_company(combined) or company in combined:
            detected.add(canonical_company(company))

    return {x for x in detected if x and x != "n a"}


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
    radius = 900 + (score * 12) + (min(mw, 500) * 3) + (min(relationships, 25) * 80)
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
    if "procurement" in title:
        score += 20
    if "utility" in title or "power" in title or "transmission" in title:
        score += 20
    if "public sector" in title:
        score += 25
    if "sales" in title or "business development" in title:
        score += 15

    return score


def influence_tier(score):
    score = safe_number(score, 0)
    if score >= 60:
        return "High Influence"
    if score >= 35:
        return "Medium Influence"
    return "Monitor"


def threat_level(score, mw, relationship_count):
    score = safe_number(score, 0)
    mw = safe_number(mw, 0)
    relationship_count = safe_number(relationship_count, 0)

    risk = 0
    if score >= 90:
        risk += 35
    if mw >= 250:
        risk += 30
    if relationship_count <= 2:
        risk += 25
    if relationship_count >= 8:
        risk -= 10

    if risk >= 70:
        return "Critical"
    if risk >= 45:
        return "Elevated"
    return "Monitor"


def deal_readiness(score, relationship_count, mw):
    score = safe_number(score, 0)
    relationship_count = safe_number(relationship_count, 0)
    mw = safe_number(mw, 0)

    readiness = 0
    if score >= 75:
        readiness += 35
    if score >= 90:
        readiness += 20
    if relationship_count >= 3:
        readiness += 20
    if relationship_count >= 8:
        readiness += 15
    if mw >= 250:
        readiness += 10

    return min(readiness, 100)


def opportunity_status(score, mw, relationship_count):
    score = safe_number(score, 0)
    mw = safe_number(mw, 0)
    relationship_count = safe_number(relationship_count, 0)

    if score >= 90 and relationship_count <= 2:
        return "CRITICAL", "#ff4b4b"
    if score >= 90:
        return "PRIME", "#00ffaa"
    if mw >= 250 and relationship_count <= 3:
        return "POWER RISK", "#ffb000"
    if score >= 75:
        return "STRATEGIC", "#008cff"
    return "MONITOR", "#8a8f98"


def recommended_actions(row, relationships_count):
    actions = []
    score = safe_number(row.get("early_capture_score"), 0)
    infrastructure_type = str(row.get("infrastructure_type") or "").lower()
    utility_dependency = clean_value(row.get("utility_dependency"), "")
    project_stage = str(row.get("project_stage") or "").lower()
    estimated_mw = clean_value(row.get("estimated_power_mw"), "")

    if score >= 90 and relationships_count <= 2:
        actions.append("Urgent: expand executive relationship coverage immediately.")
    elif score >= 90:
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
        actions.append("Prioritize outreach to highest-influence matched executive contacts.")
    else:
        actions.append("Relationship gap detected: identify owner, operator, utility, and construction stakeholders.")

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


@st.cache_data(ttl=600)
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


@st.cache_data(ttl=600)
def load_relationships():
    try:
        df = run_query(
            """
            select *
            from executive_project_matches
            order by early_capture_score desc nulls last
            """
        )

        if df.empty:
            return df

        if "title" in df.columns:
            df["influence_score"] = df["title"].apply(influence_score)
            df["influence_tier"] = df["influence_score"].apply(influence_tier)

        if "company" in df.columns:
            df["canonical_company"] = df["company"].apply(canonical_company)
        elif "company_name" in df.columns:
            df["canonical_company"] = df["company_name"].apply(canonical_company)
        else:
            df["canonical_company"] = ""

        return df

    except Exception:
        return pd.DataFrame()


def recover_relationships_for_project(project_row, relationships_df):
    if relationships_df.empty:
        return pd.DataFrame(), "No relationship data loaded"

    project_name = clean_value(project_row.get("canonical_project_name"), "")
    direct = pd.DataFrame()

    if "canonical_project_name" in relationships_df.columns:
        direct = relationships_df[
            relationships_df["canonical_project_name"].astype(str) == str(project_name)
        ].copy()

    if not direct.empty:
        direct["match_type"] = "Direct Project Match"
        return direct, "Direct project relationships found"

    project_companies = extract_project_companies(project_row)

    if not project_companies:
        return pd.DataFrame(), "No project company aliases detected"

    if "canonical_company" not in relationships_df.columns:
        return pd.DataFrame(), "No canonical company field available"

    recovered = relationships_df[
        relationships_df["canonical_company"].isin(project_companies)
    ].copy()

    if recovered.empty:
        return pd.DataFrame(), "No recovered company relationships found"

    recovered["canonical_project_name"] = project_name
    recovered["match_type"] = "Recovered Company Alias Match"
    recovered["matched_company_aliases"] = ", ".join(sorted(project_companies))

    if "influence_score" in recovered.columns:
        recovered = recovered.sort_values("influence_score", ascending=False)

    return recovered, f"Recovered through company aliases: {', '.join(sorted(project_companies))}"


def relationship_count_fast(project_row, relationships_df):
    rels, source = recover_relationships_for_project(project_row, relationships_df)
    return len(rels), source


def add_relationship_counts_for_subset(df, relationships_df):
    if df.empty:
        return df

    working_df = df.copy()
    counts = []
    sources = []

    for _, row in working_df.iterrows():
        count, source = relationship_count_fast(row, relationships_df)
        counts.append(count)
        sources.append(source)

    working_df["relationship_count"] = counts
    working_df["relationship_source"] = sources

    return working_df


def build_ribbon_df(projects_df, relationships_df):
    if projects_df.empty:
        return pd.DataFrame()

    ribbon_df = projects_df.copy().head(60)
    ribbon_df = add_relationship_counts_for_subset(ribbon_df, relationships_df)

    if "estimated_power_mw" in ribbon_df.columns:
        ribbon_df["estimated_power_mw"] = pd.to_numeric(ribbon_df["estimated_power_mw"], errors="coerce").fillna(0)
    else:
        ribbon_df["estimated_power_mw"] = 0

    ribbon_df["early_capture_score"] = pd.to_numeric(ribbon_df["early_capture_score"], errors="coerce").fillna(0)

    ribbon_df["opportunity_status"] = ribbon_df.apply(
        lambda r: opportunity_status(
            r.get("early_capture_score"),
            r.get("estimated_power_mw"),
            r.get("relationship_count"),
        )[0],
        axis=1,
    )

    ribbon_df["status_color"] = ribbon_df.apply(
        lambda r: opportunity_status(
            r.get("early_capture_score"),
            r.get("estimated_power_mw"),
            r.get("relationship_count"),
        )[1],
        axis=1,
    )

    priority_order = {
        "CRITICAL": 1,
        "PRIME": 2,
        "POWER RISK": 3,
        "STRATEGIC": 4,
        "MONITOR": 5,
    }

    ribbon_df["priority_rank"] = ribbon_df["opportunity_status"].map(priority_order).fillna(9)

    return ribbon_df.sort_values(
        by=["priority_rank", "early_capture_score", "relationship_count"],
        ascending=[True, False, True],
    )


def build_gap_report(projects_df, relationships_df):
    if projects_df.empty:
        return pd.DataFrame(columns=["canonical_project_name", "relationship_count", "relationship_source"])

    rows = []

    for _, row in projects_df.iterrows():
        name = clean_value(row.get("canonical_project_name"), "")
        count, source = relationship_count_fast(row, relationships_df)
        rows.append(
            {
                "canonical_project_name": name,
                "relationship_count": count,
                "relationship_source": source,
            }
        )

    return pd.DataFrame(rows)


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

if "ribbon_message" not in st.session_state:
    st.session_state.ribbon_message = ""

if "gap_report_df" not in st.session_state:
    st.session_state.gap_report_df = pd.DataFrame()


st.sidebar.header("Executive Command Filters")
global_search = st.sidebar.text_input("Global Intelligence Search")
prime_only = st.sidebar.checkbox("Prime Positioning Only")
predictive_only = st.sidebar.checkbox("Predictive Signals Only")
watchlist_only = st.sidebar.checkbox("Watchlist Only")
relationship_gaps_only = st.sidebar.checkbox("Relationship Gaps Only")

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
        filtered_df = filtered_df[filtered_df["canonical_project_name"].isin(st.session_state.watchlist)]

    if relationship_gaps_only and not st.session_state.gap_report_df.empty:
        gap_names = st.session_state.gap_report_df[
            st.session_state.gap_report_df["relationship_count"] == 0
        ]["canonical_project_name"].tolist()
        filtered_df = filtered_df[filtered_df["canonical_project_name"].isin(gap_names)]


st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.55rem;
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
    .ribbon-card {
        height: 255px;
        border: 1px solid #1f2a3a;
        border-top: 4px solid var(--accent);
        background: linear-gradient(145deg, #060a0f 0%, #101824 100%);
        border-radius: 12px;
        padding: 16px 16px;
        box-shadow: 0 8px 22px rgba(0,0,0,.28);
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        overflow: visible;
    }
    .ribbon-status {
        color: var(--accent);
        font-size: .72rem;
        font-weight: 900;
        letter-spacing: .08rem;
        text-transform: uppercase;
        margin-bottom: 9px;
    }
    .ribbon-title {
        color: #f5f7fa;
        font-size: .96rem;
        font-weight: 850;
        line-height: 1.18rem;
        min-height: 42px;
        max-height: 44px;
        overflow: hidden;
        margin-bottom: 10px;
    }
    .ribbon-meta {
        color: #aeb8c5;
        font-size: .76rem;
        line-height: 1.22rem;
    }
    .ribbon-action {
        color: #ffffff;
        font-size: .76rem;
        font-weight: 700;
        border-top: 1px solid #243244;
        padding-top: 10px;
        margin-top: 12px;
        min-height: 42px;
        line-height: 1.15rem;
    }
    .active-strip {
        border: 1px solid #26384f;
        border-left: 5px solid #00a3ff;
        border-radius: 10px;
        padding: 12px 16px;
        background: linear-gradient(90deg, #07111d 0%, #101824 100%);
        color: #f5f7fa;
        margin-top: 18px;
        margin-bottom: 18px;
    }
    .active-strip-title {
        font-size: .78rem;
        color: #7cc8ff;
        font-weight: 800;
        letter-spacing: .06rem;
        text-transform: uppercase;
        margin-bottom: 4px;
    }
    .active-strip-name {
        font-size: 1rem;
        font-weight: 850;
        margin-bottom: 4px;
    }
    .active-strip-meta {
        font-size: .78rem;
        color: #b5c2d1;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Infrastructure Intelligence Operating System")
st.caption("Allen Hammett AI — Executive Infrastructure Intelligence + Relationship Recovery + Capture Operations")

ribbon_df = build_ribbon_df(filtered_df, relationships_df)

st.markdown("### Executive Opportunity Ribbon")

if ribbon_df.empty:
    st.info("No active opportunity intelligence available under current filters.")
else:
    ribbon_cols = st.columns(4)
    top_ribbon = ribbon_df.head(4).reset_index(drop=True)

    for idx, item in top_ribbon.iterrows():
        project_name = clean_value(item.get("canonical_project_name"), "Unnamed Opportunity")
        status = clean_value(item.get("opportunity_status"), "MONITOR")
        status_color = clean_value(item.get("status_color"), "#8a8f98")
        score = int(safe_number(item.get("early_capture_score"), 0))
        mw = safe_number(item.get("estimated_power_mw"), 0)
        rel_count = int(safe_number(item.get("relationship_count"), 0))
        stage = clean_value(item.get("capture_stage"), "Unknown")
        market = clean_value(item.get("market_cluster"), "Unknown Market")

        if status == "CRITICAL":
            action_text = "Expand relationship coverage now."
        elif status == "PRIME":
            action_text = "Escalate BD positioning."
        elif status == "POWER RISK":
            action_text = "Validate utility and power path."
        elif status == "STRATEGIC":
            action_text = "Track capture timing."
        else:
            action_text = "Maintain monitoring posture."

        with ribbon_cols[idx]:
            st.markdown(
                f"""
                <div class="ribbon-card" style="--accent:{status_color};">
                    <div>
                        <div class="ribbon-status">{status}</div>
                        <div class="ribbon-title">{project_name}</div>
                        <div class="ribbon-meta">
                            Score: <b>{score}</b><br/>
                            MW: <b>{int(mw) if mw > 0 else "N/A"}</b><br/>
                            Relationships: <b>{rel_count}</b><br/>
                            Stage: <b>{stage}</b><br/>
                            Market: <b>{market}</b>
                        </div>
                    </div>
                    <div class="ribbon-action">{action_text}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            if st.button("Set Active Opportunity", key=f"ribbon_open_{idx}_{project_name}", use_container_width=True):
                st.session_state.selected_project = project_name
                st.session_state.ribbon_message = f"{project_name} is now the active opportunity."
                st.rerun()

active_project_name = st.session_state.selected_project

active_df = filtered_df[
    filtered_df["canonical_project_name"].astype(str) == str(active_project_name)
] if not filtered_df.empty and active_project_name else pd.DataFrame()

if not active_df.empty:
    active_row = active_df.iloc[0]
    active_rels, active_source = recover_relationships_for_project(active_row, relationships_df)

    active_score = int(safe_number(active_row.get("early_capture_score"), 0))
    active_mw = safe_number(active_row.get("estimated_power_mw"), 0)
    active_relationships = len(active_rels)
    active_threat = threat_level(active_score, active_mw, active_relationships)
    active_readiness = deal_readiness(active_score, active_relationships, active_mw)

    st.markdown(
        f"""
        <div class="active-strip">
            <div class="active-strip-title">Active Opportunity</div>
            <div class="active-strip-name">{active_project_name}</div>
            <div class="active-strip-meta">
                Score {active_score} • Readiness {int(active_readiness)}% • Threat {active_threat} • Relationships {active_relationships} • MW {int(active_mw) if active_mw > 0 else "N/A"} • {active_source}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

if st.session_state.ribbon_message:
    st.success(st.session_state.ribbon_message)


main_tab, operations_tab, deal_tab, relationships_tab, analytics_tab, exports_tab = st.tabs(
    [
        "Command Wall",
        "Opportunity Operations",
        "Deal Control",
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

    map_df = filtered_df.head(1000).copy()

    if not map_df.empty and {"latitude", "longitude"}.issubset(map_df.columns):
        map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
        map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
        map_df = map_df.dropna(subset=["latitude", "longitude"]).copy()

        if not map_df.empty:
            map_df = add_relationship_counts_for_subset(map_df, relationships_df)

            map_df["relationship_count"] = pd.to_numeric(map_df.get("relationship_count", 0), errors="coerce").fillna(0).astype(int)
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
                    map_style=pdk.map_styles.CARTO_DARK,
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

            if not st.session_state.gap_report_df.empty:
                gap_df = st.session_state.gap_report_df[st.session_state.gap_report_df["relationship_count"] == 0]
                for _, row in gap_df.head(4).iterrows():
                    alerts.append(f"Relationship gap: {clean_value(row.get('canonical_project_name'))} has no recovered contacts.")

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
        queue_df = add_relationship_counts_for_subset(queue_df, relationships_df)

        if queue_df.empty:
            st.info("No projects match current filters.")
        else:
            for idx, project in queue_df.reset_index(drop=True).iterrows():
                project_name = clean_value(project.get("canonical_project_name"), "Unnamed Project")
                score = clean_value(project.get("early_capture_score"), "0")
                rel_count = clean_value(project.get("relationship_count"), "0")
                project_id = clean_value(project.get("id"), f"row_{idx}")

                if st.button(f"{project_name} | Score {score} | Rel {rel_count}", use_container_width=True, key=f"project_select_{project_id}_{idx}"):
                    st.session_state.selected_project = project_name
                    st.session_state.ribbon_message = f"{project_name} is now the active opportunity."
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
                        st.session_state.ribbon_message = f"{watch_item} is now the active opportunity."
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
                project_relationships, match_note = recover_relationships_for_project(row, relationships_df)

                st.markdown(f"# {selected_project}")

                s1, s2, s3, s4, s5 = st.columns(5)
                s1.metric("Score", clean_value(row.get("early_capture_score")))
                s2.metric("Stage", clean_value(row.get("capture_stage")))
                s3.metric("MW", clean_value(row.get("estimated_power_mw")))
                s4.metric("Relationships", len(project_relationships))
                s5.metric("Predictive", clean_bool(row.get("predictive_signal")))

                st.caption(match_note)

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
                        st.warning("No executive relationships identified after recovery pass.")
                    else:
                        st.success(f"{len(project_relationships)} executive relationships identified")
                        st.caption(match_note)
                        relationship_columns = ["full_name", "title", "company", "email", "linkedin_url", "influence_score", "influence_tier", "match_type", "matched_company_aliases"]
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


with deal_tab:
    st.markdown("## Executive Deal Control Center")

    selected_project = st.session_state.selected_project

    if not selected_project:
        st.info("Select a project from Opportunity Operations.")
    else:
        selected_df = filtered_df[filtered_df["canonical_project_name"].astype(str) == str(selected_project)]

        if selected_df.empty:
            st.warning("Selected project is not available under current filters.")
        else:
            row = selected_df.iloc[0]
            project_relationships, match_note = recover_relationships_for_project(row, relationships_df)

            relationship_count = len(project_relationships)
            score = safe_number(row.get("early_capture_score"), 0)
            mw = safe_number(row.get("estimated_power_mw"), 0)
            threat = threat_level(score, mw, relationship_count)
            readiness = deal_readiness(score, relationship_count, mw)

            st.markdown(f"### {selected_project}")

            d1, d2, d3, d4, d5 = st.columns(5)
            d1.metric("Capture Score", int(score))
            d2.metric("Deal Readiness", f"{int(readiness)}%")
            d3.metric("MW Opportunity", int(mw) if mw > 0 else "N/A")
            d4.metric("Relationships", relationship_count)
            d5.metric("Threat Level", threat)

            st.caption(match_note)

            st.markdown("---")

            control_left, control_right = st.columns([1.5, 1])

            with control_left:
                st.markdown("### Recommended BD Moves")
                for action in recommended_actions(row, relationship_count):
                    st.success(action)

                st.markdown("### Deal Control Summary")
                st.markdown(f"**Capture Stage:** {clean_value(row.get('capture_stage'))}")
                st.markdown(f"**Project Stage:** {clean_value(row.get('project_stage'))}")
                st.markdown(f"**Infrastructure Type:** {clean_value(row.get('infrastructure_type'))}")
                st.markdown(f"**Market Cluster:** {clean_value(row.get('market_cluster'))}")
                st.markdown(f"**County:** {clean_value(row.get('county'))}")
                st.markdown(f"**Utility Dependency:** {clean_value(row.get('utility_dependency'))}")

            with control_right:
                st.markdown("### Threat Intelligence")
                if threat == "Critical":
                    st.error("High-value opportunity with insufficient relationship coverage. Immediate BD escalation recommended.")
                elif threat == "Elevated":
                    st.warning("Strategic opportunity with meaningful capture upside. Relationship and utility paths should be tightened.")
                else:
                    st.info("Opportunity remains under active monitoring. Maintain watch posture.")

                st.markdown("### Relationship Penetration")
                if relationship_count == 0:
                    st.error("No mapped or recovered executive relationships.")
                elif relationship_count < 3:
                    st.warning("Limited relationship coverage.")
                elif relationship_count < 8:
                    st.info("Moderate relationship coverage.")
                else:
                    st.success("Strong relationship coverage.")

            st.markdown("---")
            st.markdown("### Executive Influence Ranking")

            if project_relationships.empty:
                st.warning("No executive relationships mapped to this opportunity.")
            else:
                influence_cols = ["full_name", "title", "company", "email", "linkedin_url", "influence_score", "influence_tier", "match_type", "matched_company_aliases"]
                available_influence_cols = [c for c in influence_cols if c in project_relationships.columns]
                st.dataframe(project_relationships[available_influence_cols], use_container_width=True, height=450)


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

    relationship_columns = ["canonical_project_name", "company", "canonical_company", "full_name", "title", "email", "linkedin_url", "influence_score", "influence_tier"]
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
        st.markdown("### Relationship Recovery Coverage")
        if not st.session_state.gap_report_df.empty:
            coverage_df = st.session_state.gap_report_df.copy()
            coverage_df["coverage_bucket"] = pd.cut(
                coverage_df["relationship_count"],
                bins=[-1, 0, 2, 7, 10000],
                labels=["Zero", "Limited", "Moderate", "Strong"],
            )
            coverage_chart = coverage_df["coverage_bucket"].value_counts().reset_index()
            coverage_chart.columns = ["Coverage", "Projects"]
            fig = px.bar(coverage_chart, x="Coverage", y="Projects", template="plotly_dark", color="Coverage")
            fig.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Run the full gap report from Exports to populate relationship recovery analytics.")

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
            leader_cols = ["full_name", "title", "company", "canonical_company", "influence_score", "influence_tier"]
            available_leader_cols = [c for c in leader_cols if c in relationships_df.columns]
            st.dataframe(
                relationships_df[available_leader_cols]
                .sort_values("influence_score", ascending=False)
                .head(15),
                use_container_width=True,
                height=360,
            )


with exports_tab:
    st.markdown("## Export Intelligence")

    if st.button("Run Full Relationship Recovery Gap Report", key="run_gap_report"):
        with st.spinner("Running full recovery pass across all projects..."):
            st.session_state.gap_report_df = build_gap_report(projects_df, relationships_df)
        st.success("Gap report generated.")

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

    if not st.session_state.gap_report_df.empty:
        st.download_button(
            "Download Relationship Recovery Gap Report",
            st.session_state.gap_report_df.to_csv(index=False),
            "relationship_recovery_gap_report.csv",
            "text/csv",
            key="download_relationship_gap_report",
        )

    if st.button("Sign Out", key="sign_out_button"):
        st.session_state.user = None
        st.rerun()


st.caption("Allen Hammett AI • Institutional Infrastructure Intelligence Operating System")
