# app.py
# Phase 12 — Contractor Ecosystem + Relationship Intelligence Engine
# Allen Hammett AI — Infrastructure + Relationship + DEWALT Capture Intelligence

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
    page_title="Infrastructure Intelligence Operating System",
    page_icon="🛰️",
    layout="wide",
)

# -----------------------------
# Core configuration / helpers
# -----------------------------

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        port=os.getenv("PGPORT", "5432"),
    )


def run_query(sql, params=None):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        if cur.description is None:
            return pd.DataFrame()
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def run_execute(sql, params=None):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        conn.commit()


def clean_value(value, default=""):
    if value is None:
        return default
    v = str(value).strip()
    if v.lower() in ["nan", "none", "null"]:
        return default
    return v


def safe_number(value, default=0):
    try:
        if value is None:
            return default
        v = str(value).strip()
        if v.lower() in ["nan", "none", "null", ""]:
            return default
        return float(v)
    except Exception:
        return default


def normalize_text(text):
    return clean_value(text).strip().lower()


def canonical_company(name):
    name = clean_value(name)
    return re.sub(r"\s+", " ", name).strip()


def influence_score(title):
    t = normalize_text(title)
    score = 10
    if any(k in t for k in ["chief", "ceo", "president", "vp", "vice president", "partner", "principal"]):
        score += 60
    if any(k in t for k in ["director", "head", "manager"]):
        score += 30
    if any(k in t for k in ["engineer", "planner", "coordinator"]):
        score += 10
    return min(score, 100)


def influence_tier(score):
    s = safe_number(score, 0)
    if s >= 80:
        return "A"
    if s >= 55:
        return "B"
    if s >= 30:
        return "C"
    return "D"


def classify_relationship_role(title, company):
    t = normalize_text(title)
    c = normalize_text(company)

    if any(k in c for k in ["county", "city", "town", "planning", "zoning", "commission"]):
        return "Authority / AHJ"
    if any(k in t for k in ["developer", "development", "principal", "owner", "equity"]):
        return "Owner / Developer"
    if any(k in t for k in ["gc", "general contractor", "construction manager"]):
        return "GC"
    if any(k in t for k in ["mep", "electrical", "mechanical", "hvac"]):
        return "MEP"
    if any(k in t for k in ["utility", "power", "grid"]):
        return "Utility"
    if any(k in t for k in ["distributor", "distribution", "sales rep"]):
        return "Distributor"
    return "Other"


def signal_color(score):
    s = safe_number(score, 0)
    if s >= 100:
        return [255, 0, 0, 220]
    if s >= 85:
        return [255, 140, 0, 220]
    if s >= 70:
        return [255, 215, 0, 220]
    if s >= 55:
        return [0, 191, 255, 220]
    return [135, 206, 235, 180]


def signal_radius(score, mw, relationships):
    s = safe_number(score, 0)
    m = safe_number(mw, 0)
    r = safe_number(relationships, 0)
    base = 5000
    base += s * 40
    base += min(m, 500) * 15
    base += min(r, 20) * 200
    return max(3000, min(base, 60000))


def threat_level(score, mw, relationships, coverage_score):
    s = safe_number(score, 0)
    m = safe_number(mw, 0)
    r = safe_number(relationships, 0)
    c = safe_number(coverage_score, 0)

    if s >= 95 and c < 40:
        return "Severe"
    if s >= 85 and c < 50:
        return "High"
    if s >= 70 and c < 60:
        return "Elevated"
    return "Monitor"


def deal_readiness(score, relationships, mw, coverage_score):
    s = safe_number(score, 0)
    r = safe_number(relationships, 0)
    m = safe_number(mw, 0)
    c = safe_number(coverage_score, 0)

    readiness = 0.4 * s + 0.2 * min(r * 5, 40) + 0.2 * min(m / 5, 40) + 0.2 * c
    return max(0, min(round(readiness, 1), 100))


def recommended_actions(row, relationship_count, coverage_score, missing_roles):
    actions = []
    score = safe_number(row.get("early_capture_score"), 0)
    mw = safe_number(row.get("estimated_power_mw"), 0)
    stage = clean_value(row.get("capture_stage"), "Unknown")
    missing = clean_value(missing_roles, "None")

    if score >= 90:
        actions.append("Escalate to national account + field sales for immediate pursuit alignment.")
    elif score >= 75:
        actions.append("Assign regional field sales to monitor procurement timing and contractor award.")
    else:
        actions.append("Maintain monitoring posture and watch for procurement or contractor award signals.")

    if mw >= 250:
        actions.append("Validate power path and utility coordination; confirm substation / transmission constraints.")
    if relationship_count < 3:
        actions.append("Run targeted executive relationship mapping for owner, GC, MEP, and utility lanes.")
    if coverage_score < 60 and missing != "None":
        actions.append(f"Fill missing stakeholder lanes: {missing}.")
    if "permit" in stage.lower():
        actions.append("Track permit status changes and planning commission agendas for movement signals.")

    return actions


def primary_account(row):
    return canonical_company(row.get("canonical_company") or row.get("company") or "Unknown")


COVERAGE_ROLES = [
    "Owner / Developer",
    "GC",
    "MEP",
    "Utility",
    "Authority / AHJ",
    "Distributor",
    "Other",
]

PHONE_REGEX = re.compile(
    r"(?:\+1[\s\-\.]?)?(?:\(?\d{3}\)?[\s\-\.]?)\d{3}[\s\-\.]?\d{4}"
)


def extract_phone_number(text):
    text = str(text or "")
    match = PHONE_REGEX.search(text)
    return match.group(0) if match else ""


# -----------------------------
# Relationship + coverage logic
# -----------------------------

def recover_relationships_for_project(project_row, relationships_df):
    if relationships_df is None or relationships_df.empty:
        return pd.DataFrame(), "No relationship records available."

    project_name = clean_value(project_row.get("canonical_project_name"))
    company = primary_account(project_row)

    direct = relationships_df[
        relationships_df["canonical_project_name"].astype(str) == project_name
    ].copy()

    if not direct.empty:
        return direct, f"Recovered relationships directly mapped to {project_name}."

    account_matches = relationships_df[
        relationships_df["canonical_company"].astype(str) == company
    ].copy()

    if not account_matches.empty:
        return account_matches, f"Recovered relationships via strategic account match: {company}."

    fuzzy = relationships_df[
        relationships_df["company"].astype(str).str.contains(company, case=False, na=False)
    ].copy()

    if not fuzzy.empty:
        return fuzzy, f"Recovered relationships via fuzzy company match: {company}."

    return pd.DataFrame(), "No executive relationships recovered for this opportunity."


def build_coverage_summary(relationships_df):
    if relationships_df is None or relationships_df.empty:
        summary = {role: 0 for role in COVERAGE_ROLES}
        return summary, 0, "All", 0

    summary = {role: 0 for role in COVERAGE_ROLES}
    for _, row in relationships_df.iterrows():
        role = clean_value(row.get("relationship_role"), "Other")
        if role not in summary:
            summary[role] = 0
        summary[role] += 1

    filled = sum(1 for v in summary.values() if v > 0)
    coverage_score = int(round((filled / len(COVERAGE_ROLES)) * 100, 0))
    missing_roles = ", ".join([r for r, v in summary.items() if v == 0]) or "None"

    return summary, coverage_score, missing_roles, filled


def relationship_path_status(project_row, relationships_df):
    project_rels, note = recover_relationships_for_project(project_row, relationships_df)
    if project_rels.empty:
        return "No Path Yet", 0, note

    _, coverage_score, missing_roles, _ = build_coverage_summary(project_rels)

    if coverage_score >= 75:
        status = "Prime Path"
    elif coverage_score >= 50:
        status = "Emerging Path"
    else:
        status = "Weak Path"

    return status, coverage_score, f"{status} — missing lanes: {missing_roles}"


def detect_contractor_ecosystem(project_row, relationships_df):
    if relationships_df is None or relationships_df.empty:
        return pd.DataFrame()

    project_name = clean_value(project_row.get("canonical_project_name"))
    project_rels, _ = recover_relationships_for_project(project_row, relationships_df)

    if project_rels.empty:
        return pd.DataFrame()

    contractor_roles = ["GC", "MEP", "Contractor", "Subcontractor"]
    df = project_rels[
        project_rels["relationship_role"].isin(contractor_roles)
    ].copy()

    if df.empty:
        return pd.DataFrame()

    grouped = (
        df.groupby("canonical_company")
        .agg(
            contractor=("canonical_company", "first"),
            type=("relationship_role", lambda x: ", ".join(sorted(set(x)))),
            strength=("influence_score", "mean"),
            preferred_brands=("preferred_brands", lambda x: ", ".join(sorted({clean_value(v) for v in x if v})))
            if "preferred_brands" in df.columns
            else ("preferred_brands", "first"),
        )
        .reset_index(drop=True)
    )

    grouped["strength"] = grouped["strength"].round(1)
    return grouped


def labor_intensity_estimate(project_row):
    mw = safe_number(project_row.get("estimated_power_mw"), 0)
    if mw >= 400:
        return "Extreme"
    if mw >= 250:
        return "High"
    if mw >= 100:
        return "Moderate"
    return "Standard"


def procurement_stage(project_row):
    stage = normalize_text(project_row.get("capture_stage") or project_row.get("project_stage") or "")
    if any(k in stage for k in ["award", "procurement", "bid", "tender"]):
        return "Procurement / Award"
    if any(k in stage for k in ["permit", "hearing", "entitlement", "zoning"]):
        return "Entitlement / Permit"
    if any(k in stage for k in ["concept", "planning", "feasibility"]):
        return "Concept / Planning"
    return "Unknown"


def dewalt_opportunity_score(project_row, contractor_df):
    score = safe_number(project_row.get("early_capture_score"), 0)
    mw = safe_number(project_row.get("estimated_power_mw"), 0)
    contractor_hits = 0 if contractor_df is None or contractor_df.empty else len(
        contractor_df[contractor_df["contractor"] != "Unknown"]
    )

    base = min(score, 130)
    base += min(mw, 500) * 0.05
    base += min(contractor_hits, 10) * 3

    return int(max(0, min(base, 100)))


def likely_distributors(contractor_df):
    if contractor_df is None or contractor_df.empty:
        return "Unknown"
    if "likely_distributors" in contractor_df.columns:
        vals = sorted({clean_value(v) for v in contractor_df["likely_distributors"] if clean_value(v)})
        return ", ".join(vals) if vals else "Unknown"
    return "Unknown"


def tool_demand_profile(project_row):
    infra_type = normalize_text(project_row.get("infrastructure_type") or "")
    mw = safe_number(project_row.get("estimated_power_mw"), 0)

    if "data center" in infra_type:
        if mw >= 300:
            return "Mega DC — high concrete, steel, MEP, power tools"
        return "Data center — elevated MEP + power tools"
    if "substation" in infra_type or "transmission" in infra_type:
        return "Utility — power tools, grounding, line work"
    if "industrial" in infra_type or "manufacturing" in infra_type:
        return "Industrial — mix of concrete, steel, and MEP tools"
    return "General infrastructure — mixed demand"


def add_relationship_counts_for_subset(projects_df, relationships_df):
    if projects_df is None or projects_df.empty:
        return projects_df

    df = projects_df.copy()

    if relationships_df is None or relationships_df.empty:
        df["relationship_count"] = 0
        df["coverage_score"] = 0
        df["missing_roles"] = "All"
        df["relationship_path_status"] = "No Path Yet"
        return df

    rel_counts = (
        relationships_df.groupby("canonical_project_name")
        .size()
        .reset_index(name="relationship_count")
    )

    df = df.merge(rel_counts, on="canonical_project_name", how="left")
    df["relationship_count"] = df["relationship_count"].fillna(0).astype(int)

    coverage_scores = []
    missing_roles_list = []
    path_status_list = []

    for _, row in df.iterrows():
        project_rels, _ = recover_relationships_for_project(row, relationships_df)
        _, cov_score, missing_roles, _ = build_coverage_summary(project_rels)
        path_status, _, _ = relationship_path_status(row, relationships_df)
        coverage_scores.append(cov_score)
        missing_roles_list.append(missing_roles)
        path_status_list.append(path_status)

    df["coverage_score"] = coverage_scores
    df["missing_roles"] = missing_roles_list
    df["relationship_path_status"] = path_status_list

    return df


def build_gap_report(projects_df, relationships_df):
    if projects_df is None or projects_df.empty:
        return pd.DataFrame()

    subset = projects_df.copy()
    subset = add_relationship_counts_for_subset(subset, relationships_df)

    cols = [
        "canonical_project_name",
        "early_capture_score",
        "estimated_power_mw",
        "capture_stage",
        "relationship_count",
        "coverage_score",
        "missing_roles",
        "relationship_path_status",
    ]
    existing = [c for c in cols if c in subset.columns]
    return subset[existing].sort_values(
        ["coverage_score", "relationship_count", "early_capture_score"],
        ascending=[True, True, False],
    )


# -----------------------------
# Authentication / data loaders
# -----------------------------

def login_gate():
    # Lightweight gate; customize if you want real auth
    if "user" not in st.session_state:
        st.session_state.user = "executive"
    return


@st.cache_data(ttl=300)
def load_projects():
    try:
        df = run_query(
            """
            select *
            from infrastructure_projects
            order by early_capture_score desc nulls last,
                     estimated_power_mw desc nulls last
            """
        )
        return df
    except Exception as e:
        st.error(f"Project load failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_relationships():
    try:
        df = run_query(
            """
            select *
            from executive_project_matches
            order by final_score desc nulls last,
                     confidence desc nulls last,
                     imported_at desc nulls last
            """
        )

        if df.empty:
            return df

        # Influence scoring
        if "title" in df.columns:
            df["influence_score"] = df["title"].apply(influence_score)
            df["influence_tier"] = df["influence_score"].apply(influence_tier)

        # Canonical company
        if "canonical_company" not in df.columns:
            if "company" in df.columns:
                df["canonical_company"] = df["company"].apply(canonical_company)
            else:
                df["canonical_company"] = "Unknown"

        # Relationship role
        if "relationship_role" not in df.columns:
            df["relationship_role"] = df.apply(
                lambda r: classify_relationship_role(
                    r.get("title", ""),
                    r.get("company", ""),
                ),
                axis=1,
            )

        # Phase 12 — Extract + Normalize phone numbers

        # 1) Ensure a phone_number column exists
        if "phone_number" not in df.columns:
            df["phone_number"] = ""

        df["phone_number"] = df["phone_number"].fillna("").astype(str)

        # 2) Normalize from any existing phone-like columns (future‑proof)
        phone_source_cols = [c for c in ["phone_number", "phone", "mobile", "cell"] if c in df.columns]

        for col in phone_source_cols:
            df["phone_number"] = df["phone_number"].mask(
                df["phone_number"].isin(["", "nan", "None", "NULL"]),
                df[col].astype(str),
            )

        # 3) Infer phone from text fields when still missing
        text_cols = [c for c in ["raw_text", "permit_description", "strategic_notes", "notes"] if c in df.columns]

        if text_cols:
            def _infer_phone_row(row):
                existing = clean_value(row.get("phone_number"), "")
                if existing not in ["", "N/A", "nan", "None", "NULL"]:
                    return existing

                for col in text_cols:
                    candidate = extract_phone_number(row.get(col, ""))
                    if candidate:
                        return candidate

                return existing

            df["phone_number"] = df.apply(_infer_phone_row, axis=1)

        # Final clean
        df["phone_number"] = df["phone_number"].apply(lambda v: clean_value(v, ""))

        return df

    except Exception as e:
        st.error(f"Relationship load failed: {e}")
        return pd.DataFrame()


# -----------------------------
# Account intelligence builders
# -----------------------------

def build_account_profiles(projects_df, relationships_df):
    if projects_df.empty:
        return pd.DataFrame()

    profiles = projects_df.copy()
    profiles["account_name"] = profiles.apply(primary_account, axis=1)

    agg = (
        profiles.groupby("account_name")
        .agg(
            total_projects=("canonical_project_name", "nunique"),
            total_mw=("estimated_power_mw", lambda x: safe_number(x.sum(), 0)),
            avg_capture_score=("early_capture_score", "mean"),
            max_capture_score=("early_capture_score", "max"),
            prime_projects=("capture_stage", lambda x: (x == "Prime Positioning").sum()),
        )
        .reset_index()
    )

    profiles = agg.copy()

    if not relationships_df.empty and "canonical_company" in relationships_df.columns:
        rel_counts = (
            relationships_df.groupby("canonical_company")
            .size()
            .reset_index(name="relationship_inventory")
            .rename(columns={"canonical_company": "account_name"})
        )

        role_matrix = (
            relationships_df.groupby(["canonical_company", "relationship_role"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
            .rename(columns={"canonical_company": "account_name"})
        )

        profiles = profiles.merge(rel_counts, on="account_name", how="left")
        profiles = profiles.merge(role_matrix, on="account_name", how="left")
    else:
        profiles["relationship_inventory"] = 0

    profiles["relationship_inventory"] = pd.to_numeric(
        profiles["relationship_inventory"], errors="coerce"
    ).fillna(0).astype(int)

    for role in COVERAGE_ROLES:
        if role not in profiles.columns:
            profiles[role] = 0
        profiles[role] = pd.to_numeric(
            profiles[role], errors="coerce"
        ).fillna(0).astype(int)

    profiles["coverage_roles_filled"] = profiles[COVERAGE_ROLES].apply(
        lambda r: sum(1 for x in r if x > 0), axis=1
    )
    profiles["coverage_score"] = (
        (profiles["coverage_roles_filled"] / len(COVERAGE_ROLES)) * 100
    ).round(0).astype(int)
    profiles["missing_roles"] = profiles.apply(
        lambda r: ", ".join([role for role in COVERAGE_ROLES if r[role] == 0]) or "None",
        axis=1,
    )
    profiles["total_mw"] = profiles["total_mw"].fillna(0)
    profiles["avg_capture_score"] = profiles["avg_capture_score"].round(1)
    profiles["relationship_coverage_ratio"] = (
        profiles["relationship_inventory"] / profiles["total_projects"]
    ).round(2)

    profiles["account_risk"] = profiles.apply(
        lambda r: "Critical"
        if r["max_capture_score"] >= 90
        and (r["relationship_coverage_ratio"] < 3 or r["coverage_score"] < 45)
        else "Elevated"
        if r["max_capture_score"] >= 75
        and (r["relationship_coverage_ratio"] < 5 or r["coverage_score"] < 60)
        else "Monitor",
        axis=1,
    )

    profiles["account_priority"] = profiles.apply(
        lambda r: (r["max_capture_score"] * 2)
        + (r["total_projects"] * 3)
        + (min(r["total_mw"], 1000) / 10)
        - min(r["relationship_inventory"], 50)
        + (100 - r["coverage_score"]),
        axis=1,
    )

    return profiles.sort_values("account_priority", ascending=False)


def build_executive_alert_feed(projects_df, relationships_df, account_profiles_df, score_threshold=90, mw_threshold=250):
    alerts = []

    if not projects_df.empty:
        alert_projects = add_relationship_counts_for_subset(projects_df.head(120), relationships_df)

        for _, row in alert_projects.iterrows():
            project_name = clean_value(row.get("canonical_project_name"), "Unnamed Project")
            score = safe_number(row.get("early_capture_score"), 0)
            mw = safe_number(row.get("estimated_power_mw"), 0)
            relationship_count = safe_number(row.get("relationship_count"), 0)
            coverage_score = safe_number(row.get("coverage_score"), 0)
            missing_roles = clean_value(row.get("missing_roles"), "Unknown")
            path_status = clean_value(row.get("relationship_path_status"), "No Path Yet")

            if score >= score_threshold and coverage_score < 45:
                alerts.append({
                    "Priority": "Critical",
                    "Alert Type": "Prime Opportunity Coverage Gap",
                    "Subject": project_name,
                    "Signal": f"Score {int(score)} with {int(coverage_score)}% coverage and path status: {path_status}.",
                    "Recommended Action": f"Fill missing lanes: {missing_roles}.",
                })

            if score >= score_threshold and relationship_count <= 2:
                alerts.append({
                    "Priority": "Critical",
                    "Alert Type": "Relationship Penetration Failure",
                    "Subject": project_name,
                    "Signal": f"Prime opportunity with only {int(relationship_count)} recovered relationships.",
                    "Recommended Action": "Assign field sales / national account owner immediately.",
                })

            if mw >= mw_threshold and coverage_score < 60:
                alerts.append({
                    "Priority": "Elevated",
                    "Alert Type": "High MW Contractor Coverage Risk",
                    "Subject": project_name,
                    "Signal": f"{int(mw)} MW opportunity with {int(coverage_score)}% relationship coverage.",
                    "Recommended Action": "Prioritize GC, MEP, utility, and distributor relationship mapping.",
                })

    if account_profiles_df is not None and not account_profiles_df.empty:
        for _, row in account_profiles_df.head(20).iterrows():
            account = clean_value(row.get("account_name"), "Unknown Account")
            risk = clean_value(row.get("account_risk"), "Monitor")
            coverage = safe_number(row.get("coverage_score"), 0)
            projects = safe_number(row.get("total_projects"), 0)
            missing = clean_value(row.get("missing_roles"), "Unknown")
            max_score = safe_number(row.get("max_capture_score"), 0)

            if risk == "Critical":
                alerts.append({
                    "Priority": "Critical",
                    "Alert Type": "Strategic Account Risk",
                    "Subject": account,
                    "Signal": f"{int(projects)} tracked projects, max score {int(max_score)}, coverage {int(coverage)}%.",
                    "Recommended Action": f"Strengthen account lanes: {missing}.",
                })

    alert_df = pd.DataFrame(alerts)

    if alert_df.empty:
        return pd.DataFrame(columns=["Priority", "Alert Type", "Subject", "Signal", "Recommended Action"])

    priority_order = {"Critical": 1, "Elevated": 2, "Opportunity": 3, "Monitor": 4}
    alert_df["priority_rank"] = alert_df["Priority"].map(priority_order).fillna(9)

    return alert_df.sort_values(["priority_rank", "Alert Type", "Subject"]).drop(columns=["priority_rank"])


def build_ribbon_df(projects_df, relationships_df):
    if projects_df.empty:
        return pd.DataFrame()

    df = add_relationship_counts_for_subset(projects_df.copy(), relationships_df)

    df["opportunity_status"] = df.apply(
        lambda r: "CRITICAL"
        if safe_number(r.get("early_capture_score"), 0) >= 95 and safe_number(r.get("coverage_score"), 0) < 45
        else "PRIME"
        if safe_number(r.get("early_capture_score"), 0) >= 85
        else "POWER RISK"
        if safe_number(r.get("estimated_power_mw"), 0) >= 300
        else "STRATEGIC"
        if safe_number(r.get("early_capture_score"), 0) >= 70
        else "MONITOR",
        axis=1,
    )

    status_colors = {
        "CRITICAL": "#ff4b5c",
        "PRIME": "#ffb020",
        "POWER RISK": "#ff6f61",
        "STRATEGIC": "#2ecc71",
        "MONITOR": "#8a8f98",
    }
    df["status_color"] = df["opportunity_status"].map(status_colors).fillna("#8a8f98")

    df = df.sort_values(
        ["early_capture_score", "estimated_power_mw", "coverage_score"],
        ascending=[False, False, True],
    )

    return df


# -----------------------------
# App body
# -----------------------------

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

if "selected_account" not in st.session_state:
    st.session_state.selected_account = None

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
coverage_gaps_only = st.sidebar.checkbox("Coverage Gaps Only")

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

    if alert_company_keyword:
        filtered_df = filtered_df[
            filtered_df.astype(str).apply(
                lambda row: row.str.contains(alert_company_keyword, case=False, na=False).any(),
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

    if coverage_gaps_only and not st.session_state.gap_report_df.empty:
        gap_names = st.session_state.gap_report_df[
            st.session_state.gap_report_df["coverage_score"] < 45
        ]["canonical_project_name"].tolist()
        filtered_df = filtered_df[filtered_df["canonical_project_name"].isin(gap_names)]


st.markdown(
    """
    <style>
    .block-container {padding-top: 1.55rem; padding-bottom: 2rem; max-width: 1500px;}
    div[data-testid="stMetricValue"] {font-size: 1.65rem; font-weight: 700;}
    div[data-testid="stMetricLabel"] {font-size: .82rem; color: #7f8b99;}
    .stTabs [data-baseweb="tab-list"] {gap: 18px;}
    .stTabs [data-baseweb="tab"] {height: 38px; font-weight: 600;}
    .ribbon-card {
        height: 312px;
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
    .ribbon-status {color: var(--accent); font-size: .72rem; font-weight: 900; letter-spacing: .08rem; text-transform: uppercase; margin-bottom: 9px;}
    .ribbon-title {color: #f5f7fa; font-size: .96rem; font-weight: 850; line-height: 1.18rem; min-height: 42px; max-height: 44px; overflow: hidden; margin-bottom: 10px;}
    .ribbon-meta {color: #aeb8c5; font-size: .76rem; line-height: 1.22rem;}
    .ribbon-action {color: #ffffff; font-size: .76rem; font-weight: 700; border-top: 1px solid #243244; padding-top: 10px; margin-top: 12px; min-height: 42px; line-height: 1.15rem;}
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
    .active-strip-title {font-size: .78rem; color: #7cc8ff; font-weight: 800; letter-spacing: .06rem; text-transform: uppercase; margin-bottom: 4px;}
    .active-strip-name {font-size: 1rem; font-weight: 850; margin-bottom: 4px;}
    .active-strip-meta {font-size: .78rem; color: #b5c2d1;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Infrastructure Intelligence Operating System")
st.caption("Allen Hammett AI — DEWALT / Stanley Black & Decker Infrastructure Capture Intelligence")

account_profiles_global = build_account_profiles(filtered_df, relationships_df)
executive_alerts_df = build_executive_alert_feed(
    filtered_df,
    relationships_df,
    account_profiles_global,
    alert_score_threshold,
    alert_mw_threshold,
)

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
        coverage_score = int(safe_number(item.get("coverage_score"), 0))
        path_status = clean_value(item.get("relationship_path_status"), "No Path Yet")
        stage = clean_value(item.get("capture_stage"), "Unknown")
        market = clean_value(item.get("market_cluster"), "Unknown Market")

        if status == "CRITICAL":
            action_text = "Fill stakeholder coverage gaps now."
        elif status == "PRIME":
            action_text = "Escalate DEWALT / field sales positioning."
        elif status == "POWER RISK":
            action_text = "Validate utility and power path."
        elif status == "STRATEGIC":
            action_text = "Track contractor procurement timing."
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
                            Coverage: <b>{coverage_score}%</b><br/>
                            Path: <b>{path_status}</b><br/>
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
    _, active_coverage_score, active_missing_roles, _ = build_coverage_summary(active_rels)
    active_path_status, active_path_score, _ = relationship_path_status(active_row, relationships_df)
    active_coverage_score = max(active_coverage_score, active_path_score)
    active_contractor_df = detect_contractor_ecosystem(active_row, relationships_df)

    active_score = int(safe_number(active_row.get("early_capture_score"), 0))
    active_mw = safe_number(active_row.get("estimated_power_mw"), 0)
    active_relationships = len(active_rels)
    active_threat = threat_level(active_score, active_mw, active_relationships, active_coverage_score)
    active_readiness = deal_readiness(active_score, active_relationships, active_mw, active_coverage_score)
    active_dewalt_score = dewalt_opportunity_score(active_row, active_contractor_df)

    st.markdown(
        f"""
        <div class="active-strip">
            <div class="active-strip-title">Active Opportunity</div>
            <div class="active-strip-name">{active_project_name}</div>
            <div class="active-strip-meta">
                Score {active_score} • Readiness {int(active_readiness)}% • Threat {active_threat} • Relationships {active_relationships} • Coverage {active_coverage_score}% • Path {active_path_status} • DEWALT Score {active_dewalt_score}/100 • MW {int(active_mw) if active_mw > 0 else "N/A"}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

if st.session_state.ribbon_message:
    st.success(st.session_state.ribbon_message)


main_tab, alerts_tab, operations_tab, deal_tab, contractor_tab, relationships_tab, accounts_tab, analytics_tab, exports_tab = st.tabs(
    [
        "Command Wall",
        "Executive Alerts",
        "Opportunity Operations",
        "Deal Control",
        "Contractor Intelligence",
        "Relationship Command",
        "Account Intelligence",
        "Market Analytics",
        "Exports",
    ]
)


with main_tab:
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Signals", len(filtered_df))
    k2.metric("Prime", len(filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]) if not filtered_df.empty else 0)
    k3.metric("Alerts", len(executive_alerts_df))
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
            map_df["coverage_score"] = pd.to_numeric(map_df.get("coverage_score", 0), errors="coerce").fillna(0).astype(int)
            map_df["estimated_power_mw"] = pd.to_numeric(map_df.get("estimated_power_mw", 0), errors="coerce").fillna(0)
            map_df["early_capture_score"] = pd.to_numeric(map_df.get("early_capture_score", 0), errors="coerce").fillna(0)
            map_df["color"] = map_df["early_capture_score"].apply(signal_color)
            map_df["radius"] = map_df.apply(
                lambda r: signal_radius(r.get("early_capture_score", 0), r.get("estimated_power_mw", 0), r.get("relationship_count", 0)),
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
                    MW: {estimated_power_mw}<br/>
                    Relationships: {relationship_count}<br/>
                    Coverage: {coverage_score}%<br/>
                    Path: {relationship_path_status}<br/>
                    County: {county}<br/>
                    Market: {market_cluster}
                </div>
                """,
                "style": {"backgroundColor": "#05070A", "color": "#F5F7FA", "border": "1px solid #1E5EFF"},
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
        if executive_alerts_df.empty:
            st.success("No executive alerts under current thresholds.")
        else:
            for _, alert in executive_alerts_df.head(6).iterrows():
                priority = clean_value(alert.get("Priority"))
                message = f"{clean_value(alert.get('Alert Type'))}: {clean_value(alert.get('Subject'))} — {clean_value(alert.get('Signal'))}"
                if priority == "Critical":
                    st.error(message)
                elif priority == "Elevated":
                    st.warning(message)
                else:
                    st.info(message)

    with signal_col2:
        st.markdown("### Top Priority Queue")
        cols = [c for c in ["canonical_project_name", "early_capture_score", "capture_stage"] if c in filtered_df.columns]
        if not filtered_df.empty:
            st.dataframe(filtered_df[cols].head(10), use_container_width=True, height=300)


with alerts_tab:
    st.markdown("## Executive Alert Feed")
    if executive_alerts_df.empty:
        st.success("No active executive alerts under current thresholds.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Alerts", len(executive_alerts_df))
        c2.metric("Critical", len(executive_alerts_df[executive_alerts_df["Priority"] == "Critical"]))
        c3.metric("Elevated", len(executive_alerts_df[executive_alerts_df["Priority"] == "Elevated"]))

        alert_filter = st.selectbox("Alert Priority Filter", ["All", "Critical", "Elevated", "Opportunity"], index=0)
        display_alerts = executive_alerts_df.copy()
        if alert_filter != "All":
            display_alerts = display_alerts[display_alerts["Priority"] == alert_filter]
        st.dataframe(display_alerts, use_container_width=True, height=500)


with operations_tab:
    left_panel, right_panel = st.columns([1.15, 2.85])

    with left_panel:
        st.markdown("## Priority Infrastructure Queue")
        quick_search = st.text_input("Quick Project Filter", key="quick_project_filter")
        queue_df = filtered_df.copy()

        if not queue_df.empty and quick_search:
            queue_df = queue_df[
                queue_df["canonical_project_name"].astype(str).str.contains(quick_search, case=False, na=False)
            ]

        if not queue_df.empty:
            queue_df = queue_df.sort_values(by="early_capture_score", ascending=False)

        queue_limit = st.slider("Visible Queue Size", 10, 200, 40)
        queue_df = add_relationship_counts_for_subset(queue_df.head(queue_limit), relationships_df)

        if queue_df.empty:
            st.info("No projects match current filters.")
        else:
            for idx, project in queue_df.reset_index(drop=True).iterrows():
                project_name = clean_value(project.get("canonical_project_name"), "Unnamed Project")
                score = clean_value(project.get("early_capture_score"), "0")
                rel_count = clean_value(project.get("relationship_count"), "0")
                coverage_score = clean_value(project.get("coverage_score"), "0")
                path_status = clean_value(project.get("relationship_path_status"), "No Path Yet")
                project_id = clean_value(project.get("id"), f"row_{idx}")

                if st.button(
                    f"{project_name} | Score {score} | Rel {rel_count} | Cov {coverage_score}% | {path_status}",
                    use_container_width=True,
                    key=f"project_select_{project_id}_{idx}",
                ):
                    st.session_state.selected_project = project_name
                    st.session_state.ribbon_message = f"{project_name} is now the active opportunity."
                    st.rerun()

        st.markdown("---")
        st.markdown("## Watchlist")

        if st.button("⭐ Add Current Project", use_container_width=True):
            current_project = st.session_state.selected_project
            if current_project and current_project not in st.session_state.watchlist:
                st.session_state.watchlist.append(current_project)
                st.rerun()

        if not st.session_state.watchlist:
            st.info("No watchlist projects yet.")
        else:
            for idx, watch_item in enumerate(list(st.session_state.watchlist)):
                c1, c2 = st.columns([5, 1])
                with c1:
                    if st.button(watch_item, key=f"watch_select_{idx}_{watch_item}", use_container_width=True):
                        st.session_state.selected_project = watch_item
                        st.rerun()
                with c2:
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
                coverage_summary, coverage_score, missing_roles, _ = build_coverage_summary(project_relationships)
                path_status, path_score, _ = relationship_path_status(row, relationships_df)
                coverage_score = max(coverage_score, path_score)
                contractor_df = detect_contractor_ecosystem(row, relationships_df)

                st.markdown(f"# {selected_project}")

                s1, s2, s3, s4, s5, s6 = st.columns(6)
                s1.metric("Score", clean_value(row.get("early_capture_score")))
                s2.metric("Stage", clean_value(row.get("capture_stage")))
                s3.metric("MW", clean_value(row.get("estimated_power_mw")))
                s4.metric("Relationships", len(project_relationships))
                s5.metric("Coverage", f"{coverage_score}%")
                s6.metric("DEWALT Score", f"{dewalt_opportunity_score(row, contractor_df)}/100")

                st.caption(match_note)

                st.markdown("## AI Recommended Actions")
                for action in recommended_actions(row, len(project_relationships), coverage_score, missing_roles):
                    st.success(action)

                overview_tab, strategy_tab, coverage_tab, relationship_tab, contractor_inner_tab, permit_tab, raw_tab = st.tabs(
                    ["Overview", "Strategy", "Coverage", "Relationships", "Contractor Ecosystem", "Permit", "Raw Intel"]
                )

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

                with coverage_tab:
                    st.markdown("### Relationship Coverage Matrix")
                    coverage_df = pd.DataFrame(
                        [{"Coverage Lane": role, "Contacts": count, "Covered": "Yes" if count > 0 else "No"} for role, count in coverage_summary.items()]
                    )
                    st.dataframe(coverage_df, use_container_width=True, height=300)
                    st.info(f"Relationship Path Status: {path_status}")
                    if missing_roles != "None":
                        st.warning(f"Missing stakeholder lanes: {missing_roles}")
                    else:
                        st.success("All critical stakeholder lanes have at least one mapped relationship.")

                with relationship_tab:
                    st.markdown("### Executive Relationship Intelligence")
                    if project_relationships.empty:
                        st.warning("No executive relationships identified after recovery pass.")
                    else:
                        st.success(f"{len(project_relationships)} executive relationships identified")
                        st.caption(match_note)
                        cols = [
                            "full_name", "title", "company", "canonical_company", "relationship_role", "email", "phone_number",
                            "linkedin_url", "influence_score", "influence_tier", "match_type", "match_strength",
                        ]
                        existing_cols = [c for c in cols if c in project_relationships.columns]
                        st.dataframe(project_relationships[existing_cols], use_container_width=True, height=450)

                with contractor_inner_tab:
                    st.markdown("### Contractor Ecosystem Intelligence")
                    if contractor_df.empty:
                        st.warning("No contractor ecosystem identified yet.")
                    else:
                        st.dataframe(contractor_df, use_container_width=True)

                    st.success(
                        f"""
Labor Scale: {labor_intensity_estimate(row)}

Procurement Stage: {procurement_stage(row)}

DEWALT Opportunity Score: {dewalt_opportunity_score(row, contractor_df)}/100

Likely Distributor Ecosystem: {likely_distributors(contractor_df)}

Tool Demand Profile: {tool_demand_profile(row)}
"""
                    )

                with permit_tab:
                    st.markdown("### Permit Intelligence")
                    st.markdown(f"**Applicant:** {clean_value(row.get('applicant_name'))}")
                    st.markdown(f"**Source Name:** {clean_value(row.get('source_name'))}")
                    st.markdown(f"**Source Type:** {clean_value(row.get('source_type'))}")
                    st.markdown(f"**Filing Date:** {clean_value(row.get('filing_date'))}")
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
            coverage_summary, coverage_score, missing_roles, _ = build_coverage_summary(project_relationships)
            path_status, path_score, _ = relationship_path_status(row, relationships_df)
            coverage_score = max(coverage_score, path_score)
            contractor_df = detect_contractor_ecosystem(row, relationships_df)

            relationship_count = len(project_relationships)
            score = safe_number(row.get("early_capture_score"), 0)
            mw = safe_number(row.get("estimated_power_mw"), 0)
            threat = threat_level(score, mw, relationship_count, coverage_score)
            readiness = deal_readiness(score, relationship_count, mw, coverage_score)
            dewalt_score = dewalt_opportunity_score(row, contractor_df)

            st.markdown(f"### {selected_project}")

            d1, d2, d3, d4, d5, d6 = st.columns(6)
            d1.metric("Capture Score", int(score))
            d2.metric("Deal Readiness", f"{int(readiness)}%")
            d3.metric("MW Opportunity", int(mw) if mw > 0 else "N/A")
            d4.metric("Relationships", relationship_count)
            d5.metric("Coverage", f"{coverage_score}%")
            d6.metric("DEWALT Score", f"{dewalt_score}/100")

            st.caption(match_note)
            st.info(f"Relationship Path Status: {path_status}")

            st.markdown("### Contractor Procurement Intelligence")
            if contractor_df.empty:
                st.warning("No contractor ecosystem identified yet.")
            else:
                st.dataframe(contractor_df, use_container_width=True)

            c1, c2, c3 = st.columns(3)
            c1.metric("Labor Scale", labor_intensity_estimate(row))
            c2.metric("Procurement Stage", procurement_stage(row))
            c3.metric("Tool Demand", tool_demand_profile(row))

            st.markdown("### Recommended DEWALT / SBD Moves")
            for action in recommended_actions(row, relationship_count, coverage_score, missing_roles):
                st.success(action)

            if dewalt_score >= 80:
                st.success("High-value DEWALT pursuit. Align national account, field sales, and distributor teams.")
            elif dewalt_score >= 55:
                st.warning("Moderate DEWALT opportunity. Monitor contractor award and MEP mobilization.")
            else:
                st.info("Monitor until contractor ecosystem or field procurement signals strengthen.")


with contractor_tab:
    st.markdown("## Contractor Ecosystem Intelligence")

    contractor_rows = []

    scan_df = filtered_df.head(500).copy()

    for _, row in scan_df.iterrows():
        contractor_df = detect_contractor_ecosystem(row, relationships_df)
        dewalt_score = dewalt_opportunity_score(row, contractor_df)

        if contractor_df.empty:
            contractor_rows.append({
                "project": clean_value(row.get("canonical_project_name")),
                "contractor": "Unknown",
                "contractor_type": "Unknown",
                "dewalt_score": dewalt_score,
                "labor_intensity": labor_intensity_estimate(row),
                "procurement_stage": procurement_stage(row),
                "tool_demand": tool_demand_profile(row),
                "likely_distributors": "Unknown",
            })
        else:
            for _, c in contractor_df.iterrows():
                contractor_rows.append({
                    "project": clean_value(row.get("canonical_project_name")),
                    "contractor": c["contractor"],
                    "contractor_type": c["type"],
                    "contractor_strength": c["strength"],
                    "preferred_brands": c["preferred_brands"],
                    "dewalt_score": dewalt_score,
                    "labor_intensity": labor_intensity_estimate(row),
                    "procurement_stage": procurement_stage(row),
                    "tool_demand": tool_demand_profile(row),
                    "likely_distributors": likely_distributors(contractor_df),
                })

    contractor_intel_df = pd.DataFrame(contractor_rows)

    if contractor_intel_df.empty:
        st.info("No contractor intelligence generated.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Scanned Projects", contractor_intel_df["project"].nunique())
        c2.metric("Known Contractor Hits", len(contractor_intel_df[contractor_intel_df["contractor"] != "Unknown"]))
        c3.metric("Avg DEWALT Score", int(contractor_intel_df["dewalt_score"].mean()))
        c4.metric("High Priority", len(contractor_intel_df[contractor_intel_df["dewalt_score"] >= 80]))

        st.dataframe(
            contractor_intel_df.sort_values("dewalt_score", ascending=False),
            use_container_width=True,
            height=620,
        )


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

    relationship_columns = [
        "canonical_project_name", "company", "canonical_company", "full_name", "title",
        "relationship_role", "email", "phone_number", "linkedin_url", "city", "state",
        "influence_score", "influence_tier", "research_status",
    ]
    existing_cols = [c for c in relationship_columns if c in display_relationships.columns]

    if display_relationships.empty:
        st.info("No executive relationships found.")
    else:
        st.dataframe(display_relationships[existing_cols], use_container_width=True, height=650)


with accounts_tab:
    st.markdown("## Strategic Account Intelligence")

    account_profiles = account_profiles_global

    if account_profiles.empty:
        st.info("No account intelligence available.")
    else:
        top_accounts = account_profiles.head(30)

        a1, a2, a3, a4, a5, a6 = st.columns(6)
        a1.metric("Strategic Accounts", len(account_profiles))
        a2.metric("Tracked Projects", int(account_profiles["total_projects"].sum()))
        a3.metric("Known Relationships", int(account_profiles["relationship_inventory"].sum()))
        a4.metric("Prime Projects", int(account_profiles["prime_projects"].sum()))
        a5.metric("Total MW", int(account_profiles["total_mw"].sum()))
        a6.metric("Avg Coverage", f"{int(account_profiles['coverage_score'].mean())}%")

        st.markdown("### Account Priority Board")
        st.dataframe(
            top_accounts[
                [
                    "account_name",
                    "total_projects",
                    "total_mw",
                    "avg_capture_score",
                    "max_capture_score",
                    "relationship_inventory",
                    "relationship_coverage_ratio",
                    "coverage_score",
                    "missing_roles",
                    "account_risk",
                ]
            ],
            use_container_width=True,
            height=420,
        )

        selected_account = st.selectbox("Select Strategic Account", top_accounts["account_name"].tolist(), index=0)
        selected_profile = account_profiles[account_profiles["account_name"] == selected_account].iloc[0]

        st.markdown(f"## {selected_account} Account Control")

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Projects", int(selected_profile["total_projects"]))
        c2.metric("Total MW", int(selected_profile["total_mw"]))
        c3.metric("Max Score", int(selected_profile["max_capture_score"]))
        c4.metric("Relationships", int(selected_profile["relationship_inventory"]))
        c5.metric("Coverage", f"{int(selected_profile['coverage_score'])}%")
        c6.metric("Risk", selected_profile["account_risk"])

        if selected_profile["account_risk"] == "Critical":
            st.error(f"Account risk is critical. Missing lanes: {selected_profile['missing_roles']}")
        elif selected_profile["account_risk"] == "Elevated":
            st.warning(f"Account requires attention. Missing lanes: {selected_profile['missing_roles']}")
        else:
            st.success("Account is in monitoring posture.")

        st.markdown("### Account Coverage Matrix")
        account_coverage_df = pd.DataFrame(
            [{"Coverage Lane": role, "Contacts": int(selected_profile[role]), "Covered": "Yes" if int(selected_profile[role]) > 0 else "No"} for role in COVERAGE_ROLES]
        )
        st.dataframe(account_coverage_df, use_container_width=True, height=300)

        account_relationships = pd.DataFrame()
        if not relationships_df.empty and "canonical_company" in relationships_df.columns:
            account_relationships = relationships_df[relationships_df["canonical_company"] == selected_account].copy()

        st.markdown("### Account Relationship Inventory")
        if account_relationships.empty:
            st.warning("No executive relationships currently mapped to this account.")
        else:
            rel_cols = ["full_name", "title", "company", "relationship_role", "email", "phone_number", "linkedin_url", "influence_score", "influence_tier"]
            available_rel_cols = [c for c in rel_cols if c in account_relationships.columns]
            st.dataframe(
                account_relationships[available_rel_cols].sort_values("influence_score", ascending=False).head(100),
                use_container_width=True,
                height=360,
            )


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
        st.markdown("### Relationship Role Distribution")
        if not relationships_df.empty and "relationship_role" in relationships_df.columns:
            role_chart = relationships_df["relationship_role"].value_counts().reset_index()
            role_chart.columns = ["Role", "Contacts"]
            fig = px.bar(role_chart, x="Role", y="Contacts", template="plotly_dark", color="Role")
            fig.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


with exports_tab:
    st.markdown("## Export Intelligence")

    if st.button("Run Full Relationship Recovery Gap Report"):
        with st.spinner("Running full recovery pass across all projects..."):
            st.session_state.gap_report_df = build_gap_report(projects_df, relationships_df)
        st.success("Gap report generated.")

    if not filtered_df.empty:
        st.download_button(
            "Download Infrastructure Intelligence CSV",
            filtered_df.to_csv(index=False),
            "infrastructure_intelligence.csv",
            "text/csv",
        )

    if not relationships_df.empty:
        st.download_button(
            "Download Executive Relationship Pipeline CSV",
            relationships_df.to_csv(index=False),
            "executive_relationship_pipeline.csv",
            "text/csv",
        )

    if not account_profiles_global.empty:
        st.download_button(
            "Download Strategic Account Intelligence CSV",
            account_profiles_global.to_csv(index=False),
            "strategic_account_intelligence.csv",
            "text/csv",
        )

    if not executive_alerts_df.empty:
        st.download_button(
            "Download Executive Alert Feed CSV",
            executive_alerts_df.to_csv(index=False),
            "executive_alert_feed.csv",
            "text/csv",
        )

    if not st.session_state.gap_report_df.empty:
        st.download_button(
            "Download Relationship Coverage Gap Report",
            st.session_state.gap_report_df.to_csv(index=False),
            "relationship_coverage_gap_report.csv",
            "text/csv",
        )

    if st.button("Sign Out"):
        st.session_state.user = None
        st.rerun()


st.caption("Allen Hammett AI • DEWALT / Stanley Black & Decker Infrastructure Capture Intelligence Operating System")
