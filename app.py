# app.py
# Phase 11B — Contractor Ecosystem + DEWALT Demand Sequencing Intelligence Layer
# Allen Hammett AI — Infrastructure + Relationship + DEWALT Capture Intelligence
# NOTE: Full script regeneration per request. UI/tabs preserved exactly.

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
PASSWORD_RESET_KEY = st.secrets.get(
    "PASSWORD_RESET_KEY",
    os.getenv("PASSWORD_RESET_KEY", "AH_RESET_2026"),
)

# Phase 11B feature flag (safe toggle)
ENABLE_PHASE_11B = str(st.secrets.get("ENABLE_PHASE_11B", os.getenv("ENABLE_PHASE_11B", "false"))).lower() == "true"


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


def normalize_text(value):
    text = clean_value(value, "").lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def canonical_company(value):
    text = normalize_text(value)

    aliases = {
        "Aligned Data Centers": ["aligned", "aligned data centers", "aligneddc", "aligned energy data center"],
        "Vantage": ["vantage", "vantage data centers", "vantage dc"],
        "QTS": ["qts", "qts data centers", "quality technology services"],
        "STACK Infrastructure": ["stack", "stack infrastructure", "stackinfra"],
        "Digital Realty": ["digital realty", "digitalrealty", "digital realty trust"],
        "Equinix": ["equinix"],
        "CyrusOne": ["cyrusone", "cyrus one"],
        "CoreSite": ["coresite", "core site"],
        "Cologix": ["cologix"],
        "Dominion Energy": ["dominion", "dominion energy", "dominionenergy"],
        "NOVEC": ["novec", "northern virginia electric cooperative"],
        "DPR Construction": ["dpr", "dpr construction"],
        "Turner Construction": ["turner", "turner construction"],
        "HITT": ["hitt", "hitt contracting"],
        "Clayco": ["clayco"],
        "Whiting-Turner": ["whiting turner", "whiting-turner"],
        "Holder Construction": ["holder", "holder construction"],
        "JE Dunn": ["je dunn", "j e dunn"],
        "Fortis Construction": ["fortis", "fortis construction"],
        "Rosendin": ["rosendin"],
        "M.C. Dean": ["m c dean", "mc dean", "m.c. dean"],
        "Dynaelectric": ["dynaelectric"],
        "Cupertino Electric": ["cupertino electric"],
        "Faith Technologies": ["faith technologies"],
        "The Bell Company": ["bell company", "the bell company"],
        "Graybar": ["graybar"],
        "Wesco": ["wesco"],
        "White Cap": ["white cap"],
        "Fastenal": ["fastenal"],
        "Grainger": ["grainger"],
        "HD Supply": ["hd supply"],
        "Burns & McDonnell": ["burns mcdonnell", "burns and mcdonnell"],
        "Jacobs": ["jacobs"],
        "HDR": ["hdr"],
        "Black & Veatch": ["black veatch", "black and veatch"],
        "Compass Datacenters": ["compass", "compass datacenters", "compass data centers"],
        "NTT Global Data Centers": ["ntt", "ntt global data centers"],
        "EdgeCore": ["edgecore", "edgecore digital infrastructure"],
        "DataBank": ["databank", "data bank"],
    }

    for canonical, keys in aliases.items():
        for key in keys:
            if key in text:
                return canonical

    return clean_value(value, "Unknown")


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
                company = canonical_company(value)
                if company not in ["N/A", "Unknown", ""]:
                    detected.add(company)

    return detected


def primary_account(row):
    companies = list(extract_project_companies(row))
    if companies:
        priority = [
            "Aligned Data Centers",
            "Vantage",
            "QTS",
            "STACK Infrastructure",
            "Digital Realty",
            "Equinix",
            "CyrusOne",
            "CoreSite",
            "Cologix",
            "Dominion Energy",
            "NOVEC",
        ]
        for item in priority:
            if item in companies:
                return item
        return companies[0]
    return "Unknown Account"


def classify_relationship_role(title, company=""):
    title_text = normalize_text(title)
    company_text = normalize_text(company)

    if any(x in company_text for x in ["dominion", "novec", "utility", "electric"]):
        return "Utility"
    if any(x in company_text for x in ["dpr", "turner", "hitt", "clayco", "whiting", "holder", "fortis"]):
        return "Construction"
    if any(x in company_text for x in ["rosendin", "dean", "dynaelectric", "cupertino", "faith"]):
        return "Electrical / MEP"
    if any(x in title_text for x in ["utility", "power", "transmission", "electric"]):
        return "Utility"
    if any(x in title_text for x in ["construction", "preconstruction", "mission critical", "superintendent"]):
        return "Construction"
    if any(x in title_text for x in ["engineer", "engineering", "design", "technical"]):
        return "Engineering"
    if any(x in title_text for x in ["procurement", "sourcing", "vendor", "supply"]):
        return "Procurement"
    if any(x in title_text for x in ["operations", "facilities", "facility", "data center manager"]):
        return "Operations"
    if any(x in title_text for x in ["development", "site acquisition", "real estate", "strategic accounts"]):
        return "Owner / Developer"
    if any(x in title_text for x in ["county", "planning", "zoning", "public sector", "government"]):
        return "Government"
    return "General Executive"


COVERAGE_ROLES = [
    "Owner / Developer",
    "Utility",
    "Construction",
    "Engineering",
    "Procurement",
    "Operations",
    "Government",
]


def build_coverage_summary(relationships):
    summary = {role: 0 for role in COVERAGE_ROLES}

    if relationships is None or relationships.empty:
        return summary, 0, ", ".join(COVERAGE_ROLES), "Critical"

    df = relationships.copy()

    if "relationship_role" not in df.columns:
        df["relationship_role"] = df.apply(
            lambda r: classify_relationship_role(r.get("title", ""), r.get("company", "")),
            axis=1,
        )

    for role in COVERAGE_ROLES:
        summary[role] = int((df["relationship_role"].astype(str) == role).sum())

    covered_roles = sum(1 for _, count in summary.items() if count > 0)
    coverage_score = int(round((covered_roles / len(COVERAGE_ROLES)) * 100, 0))
    missing_roles = [role for role, count in summary.items() if count == 0]

    if coverage_score >= 75:
        coverage_status = "Strong"
    elif coverage_score >= 45:
        coverage_status = "Moderate"
    elif coverage_score >= 20:
        coverage_status = "Weak"
    else:
        coverage_status = "Critical"

    return summary, coverage_score, ", ".join(missing_roles) if missing_roles else "None", coverage_status


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
    radius = 900 + (safe_number(score) * 12) + (min(safe_number(mw), 500) * 3) + (min(safe_number(relationships), 25) * 80)
    return int(radius) if radius > 0 else 1200


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
    if "superintendent" in title or "field" in title:
        score += 25
    if "public sector" in title:
        score += 25
    return score


def influence_tier(score):
    score = safe_number(score, 0)
    if score >= 60:
        return "High Influence"
    if score >= 35:
        return "Medium Influence"
    return "Monitor"


def threat_level(score, mw, relationship_count, coverage_score=0):
    risk = 0
    if safe_number(score) >= 90:
        risk += 35
    if safe_number(mw) >= 250:
        risk += 30
    if safe_number(relationship_count) <= 2:
        risk += 25
    if safe_number(coverage_score) < 35:
        risk += 20
    if safe_number(relationship_count) >= 8:
        risk -= 10
    if safe_number(coverage_score) >= 75:
        risk -= 15

    if risk >= 70:
        return "Critical"
    if risk >= 45:
        return "Elevated"
    return "Monitor"


def deal_readiness(score, relationship_count, mw, coverage_score=0):
    readiness = 0
    if safe_number(score) >= 75:
        readiness += 30
    if safe_number(score) >= 90:
        readiness += 15
    if safe_number(relationship_count) >= 3:
        readiness += 15
    if safe_number(relationship_count) >= 8:
        readiness += 10
    if safe_number(coverage_score) >= 45:
        readiness += 15
    if safe_number(coverage_score) >= 75:
        readiness += 15
    if safe_number(mw) >= 250:
        readiness += 10
    return min(readiness, 100)


def opportunity_status(score, mw, relationship_count, coverage_score=0):
    if safe_number(score) >= 90 and (safe_number(relationship_count) <= 2 or safe_number(coverage_score) < 35):
        return "CRITICAL", "#ff4b4b"
    if safe_number(score) >= 90:
        return "PRIME", "#00ffaa"
    if safe_number(mw) >= 250 and (safe_number(relationship_count) <= 3 or safe_number(coverage_score) < 45):
        return "POWER RISK", "#ffb000"
    if safe_number(score) >= 75:
        return "STRATEGIC", "#008cff"
    return "MONITOR", "#8a8f98"


def recommended_actions(row, relationships_count, coverage_score=0, missing_roles=""):
    actions = []
    score = safe_number(row.get("early_capture_score"), 0)
    infrastructure_type = normalize_text(row.get("infrastructure_type"))
    project_stage = normalize_text(row.get("project_stage"))

    if score >= 90 and relationships_count <= 2:
        actions.append("Urgent: expand executive and field relationship coverage immediately.")
    elif score >= 90:
        actions.append("Escalate national account / field sales positioning.")
    elif score >= 75:
        actions.append("Track as strategic contractor ecosystem opportunity.")

    if coverage_score < 45:
        actions.append(f"Coverage gap: fill missing lanes — {missing_roles}.")
    elif coverage_score < 75:
        actions.append(f"Improve capture readiness by strengthening missing lanes — {missing_roles}.")
    else:
        actions.append("Relationship coverage is strong enough for active capture coordination.")

    if "data center" in infrastructure_type:
        actions.append("Prioritize DEWALT / Stanley Black & Decker contractor penetration strategy.")

    if "review" in project_stage or "submitted" in project_stage:
        actions.append("Procurement window is approaching: monitor GC/MEP award and mobilization signals.")

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
        df["estimated_power_mw"] = pd.to_numeric(df["estimated_power_mw"], errors="coerce").fillna(0)
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
            order by final_score desc nulls last,
                     confidence desc nulls last,
                     imported_at desc nulls last
            """
        )

        if df.empty:
            return df

        if "title" in df.columns:
            df["influence_score"] = df["title"].apply(influence_score)
            df["influence_tier"] = df["influence_score"].apply(influence_tier)

        if "canonical_company" not
