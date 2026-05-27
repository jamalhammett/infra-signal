# app.py
# Phase 10D — Relationship Mapping Truth Layer

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
                if company and company not in ["N/A", "Unknown"]:
                    detected.add(company)

    combined = " ".join([normalize_text(row.get(field)) for field in candidate_fields if field in row.index])

    known_companies = [
        "Aligned Data Centers", "Vantage", "QTS", "STACK Infrastructure", "Digital Realty",
        "Equinix", "CyrusOne", "CoreSite", "Cologix", "Dominion Energy", "NOVEC",
        "DPR Construction", "Turner Construction", "HITT", "Clayco", "Whiting-Turner",
        "Burns & McDonnell", "Jacobs", "HDR", "Black & Veatch", "Compass Datacenters",
        "NTT Global Data Centers", "EdgeCore", "DataBank",
    ]

    for company in known_companies:
        if normalize_text(company) in combined:
            detected.add(company)

    return detected


def primary_account(row):
    companies = list(extract_project_companies(row))
    if companies:
        priority = [
            "Aligned Data Centers", "Vantage", "QTS", "STACK Infrastructure", "Digital Realty",
            "Equinix", "CyrusOne", "CoreSite", "Cologix", "Dominion Energy", "NOVEC",
        ]
        for item in priority:
            if item in companies:
                return item
        return companies[0]
    return "Unknown Account"


def classify_relationship_role(title, company=""):
    title_text = normalize_text(title)
    company_text = normalize_text(company)

    if any(x in company_text for x in ["dominion", "novec", "duke", "apco", "utility", "electric"]):
        return "Utility"
    if any(x in company_text for x in ["dpr", "turner", "hitt", "clayco", "whiting"]):
        return "Construction"
    if any(x in company_text for x in ["burns", "jacobs", "hdr", "veatch", "engineering"]):
        return "Engineering"
    if any(x in title_text for x in ["utility", "power", "transmission", "electric", "large load", "distribution"]):
        return "Utility"
    if any(x in title_text for x in ["construction", "preconstruction", "project construction", "mission critical", "mep"]):
        return "Construction"
    if any(x in title_text for x in ["engineer", "engineering", "design", "technical"]):
        return "Engineering"
    if any(x in title_text for x in ["procurement", "sourcing", "vendor", "supply", "logistics"]):
        return "Procurement"
    if any(x in title_text for x in ["operations", "facilities", "facility", "data center manager", "critical infrastructure"]):
        return "Operations"
    if any(x in title_text for x in ["development", "site acquisition", "real estate", "portfolio", "business development", "strategic accounts"]):
        return "Owner / Developer"
    if any(x in title_text for x in ["county", "planning", "zoning", "economic development", "public sector", "government"]):
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
        company_col = "company" if "company" in df.columns else "company_name" if "company_name" in df.columns else None
        df["relationship_role"] = df.apply(
            lambda r: classify_relationship_role(r.get("title", ""), r.get(company_col, "") if company_col else ""),
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
    score = safe_number(score, 0)
    mw = safe_number(mw, 0)
    relationships = safe_number(relationships, 0)
    radius = 900 + (score * 12) + (min(mw, 500) * 3) + (min(relationships, 25) * 80)
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


def threat_level(score, mw, relationship_count, coverage_score=0):
    score = safe_number(score, 0)
    mw = safe_number(mw, 0)
    relationship_count = safe_number(relationship_count, 0)
    coverage_score = safe_number(coverage_score, 0)

    risk = 0
    if score >= 90:
        risk += 35
    if mw >= 250:
        risk += 30
    if relationship_count <= 2:
        risk += 25
    if coverage_score < 35:
        risk += 20
    if relationship_count >= 8:
        risk -= 10
    if coverage_score >= 75:
        risk -= 15

    if risk >= 70:
        return "Critical"
    if risk >= 45:
        return "Elevated"
    return "Monitor"


def deal_readiness(score, relationship_count, mw, coverage_score=0):
    score = safe_number(score, 0)
    relationship_count = safe_number(relationship_count, 0)
    mw = safe_number(mw, 0)
    coverage_score = safe_number(coverage_score, 0)

    readiness = 0
    if score >= 75:
        readiness += 30
    if score >= 90:
        readiness += 15
    if relationship_count >= 3:
        readiness += 15
    if relationship_count >= 8:
        readiness += 10
    if coverage_score >= 45:
        readiness += 15
    if coverage_score >= 75:
        readiness += 15
    if mw >= 250:
        readiness += 10

    return min(readiness, 100)


def opportunity_status(score, mw, relationship_count, coverage_score=0):
    score = safe_number(score, 0)
    mw = safe_number(mw, 0)
    relationship_count = safe_number(relationship_count, 0)
    coverage_score = safe_number(coverage_score, 0)

    if score >= 90 and (relationship_count <= 2 or coverage_score < 35):
        return "CRITICAL", "#ff4b4b"
    if score >= 90:
        return "PRIME", "#00ffaa"
    if mw >= 250 and (relationship_count <= 3 or coverage_score < 45):
        return "POWER RISK", "#ffb000"
    if score >= 75:
        return "STRATEGIC", "#008cff"
    return "MONITOR", "#8a8f98"


def recommended_actions(row, relationships_count, coverage_score=0, missing_roles=""):
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

    if coverage_score < 45:
        actions.append(f"Coverage gap: fill missing stakeholder lanes — {missing_roles}.")
    elif coverage_score < 75:
        actions.append(f"Improve capture readiness by strengthening missing lanes — {missing_roles}.")
    else:
        actions.append("Relationship coverage is strong enough for active capture coordination.")

    if "data center" in infrastructure_type:
        actions.append("Assess hyperscale ecosystem: utility, fiber, security, compliance, and construction stakeholders.")

    if utility_dependency not in ["", "N/A", "Unknown"]:
        actions.append(f"Map utility relationship path connected to {utility_dependency}.")

    if estimated_mw not in ["", "N/A", "Unknown"]:
        actions.append("Evaluate power availability, transmission risk, and substation proximity.")

    if relationships_count > 0:
        actions.append("Prioritize outreach to highest-influence matched executive contacts.")
    else:
        actions.append("Relationship gap detected: identify owner, operator, utility, and construction stakeholders.")

    if "review" in project_stage or "submitted" in project_stage:
        actions.append("Monitor permit approval timeline and county planning updates.")

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

        if "canonical_company" not in df.columns:
            if "company" in df.columns:
                df["canonical_company"] = df["company"].apply(canonical_company)
            else:
                df["canonical_company"] = "Unknown"

        if "relationship_role" not in df.columns:
            company_col = "company" if "company" in df.columns else None
            df["relationship_role"] = df.apply(
                lambda r: classify_relationship_role(
                    r.get("title", ""),
                    r.get(company_col, "") if company_col else "",
                ),
                axis=1,
            )

        return df

    except Exception as e:
        st.error(f"Relationship load failed: {e}")
        return pd.DataFrame()


def recover_relationships_for_project(project_row, relationships_df):
    if relationships_df.empty:
        return pd.DataFrame(), "No relationship data loaded"

    project_name = clean_value(project_row.get("canonical_project_name"), "")
    project_text = normalize_text(" ".join([
        clean_value(project_row.get("canonical_project_name"), ""),
        clean_value(project_row.get("project_name"), ""),
        clean_value(project_row.get("applicant_name"), ""),
        clean_value(project_row.get("company_name"), ""),
        clean_value(project_row.get("owner"), ""),
        clean_value(project_row.get("developer"), ""),
        clean_value(project_row.get("utility_dependency"), ""),
        clean_value(project_row.get("permit_description"), ""),
        clean_value(project_row.get("raw_text"), ""),
    ]))

    rel_df = relationships_df.copy()

    for col in ["canonical_project_name", "company", "canonical_company", "title", "relationship_role", "final_score", "confidence"]:
        if col not in rel_df.columns:
            rel_df[col] = ""

    rel_df["match_strength"] = 0
    rel_df["match_type"] = "Unmapped"

    direct_mask = rel_df["canonical_project_name"].astype(str).str.lower() == str(project_name).lower()
    rel_df.loc[direct_mask, "match_strength"] = 100
    rel_df.loc[direct_mask, "match_type"] = "Direct Project Match"

    project_companies = extract_project_companies(project_row)
    project_companies = {canonical_company(x) for x in project_companies if x}

    for company in project_companies:
        company_mask = rel_df["canonical_company"].astype(str).str.lower() == str(company).lower()
        rel_df.loc[company_mask & (rel_df["match_strength"] < 90), "match_strength"] = 90
        rel_df.loc[company_mask & (rel_df["match_type"] == "Unmapped"), "match_type"] = "Strategic Account Match"

    for idx, rel in rel_df.iterrows():
        rel_company = normalize_text(rel.get("canonical_company") or rel.get("company"))
        if rel_company and rel_company in project_text:
            if rel_df.at[idx, "match_strength"] < 80:
                rel_df.at[idx, "match_strength"] = 80
                rel_df.at[idx, "match_type"] = "Company Text Match"

    utility_terms = ["dominion", "novec", "electric", "utility", "power", "transmission"]
    if any(term in project_text for term in utility_terms):
        utility_mask = rel_df["relationship_role"].astype(str).str.lower().str.contains("utility|power|transmission", na=False)
        rel_df.loc[utility_mask & (rel_df["match_strength"] < 70), "match_strength"] = 70
        rel_df.loc[utility_mask & (rel_df["match_type"] == "Unmapped"), "match_type"] = "Utility Path Match"

    contractor_terms = ["construction", "mission critical", "dpr", "turner", "hitt", "clayco"]
    if any(term in project_text for term in contractor_terms):
        contractor_mask = rel_df["relationship_role"].astype(str).str.lower().str.contains("construction|engineering|operations", na=False)
        rel_df.loc[contractor_mask & (rel_df["match_strength"] < 60), "match_strength"] = 60
        rel_df.loc[contractor_mask & (rel_df["match_type"] == "Unmapped"), "match_type"] = "Contractor / Delivery Path Match"

    recovered = rel_df[rel_df["match_strength"] > 0].copy()

    if recovered.empty:
        return pd.DataFrame(), "No relationship path identified"

    for col in ["final_score", "confidence"]:
        recovered[col] = pd.to_numeric(recovered[col], errors="coerce").fillna(0)

    recovered = recovered.sort_values(
        by=["match_strength", "final_score", "confidence"],
        ascending=False,
    )

    best_match = recovered.iloc[0]["match_type"]
    return recovered, f"Relationship path identified: {best_match}"


def relationship_path_status(project_row, relationships_df):
CONTRACTOR_ECOSYSTEM = {
    "DPR Construction": {
        "type": "Prime GC",
        "strength": 95,
        "preferred_brands": ["DEWALT", "Milwaukee", "Hilti"],
        "distributors": ["White Cap", "Fastenal", "Grainger"],
    },
    "Turner Construction": {
        "type": "Prime GC",
        "strength": 94,
        "preferred_brands": ["DEWALT", "Milwaukee"],
        "distributors": ["White Cap", "HD Supply", "Grainger"],
    },
    "HITT": {
        "type": "Prime GC",
        "strength": 88,
        "preferred_brands": ["DEWALT", "Milwaukee"],
        "distributors": ["White Cap", "Fastenal"],
    },
    "Clayco": {
        "type": "Prime GC",
        "strength": 86,
        "preferred_brands": ["DEWALT"],
        "distributors": ["White Cap", "Grainger"],
    },
    "Whiting-Turner": {
        "type": "Prime GC",
        "strength": 90,
        "preferred_brands": ["Milwaukee", "DEWALT"],
        "distributors": ["Fastenal", "Grainger"],
    },
    "Rosendin": {
        "type": "MEP / Electrical",
        "strength": 91,
        "preferred_brands": ["DEWALT", "Milwaukee"],
        "distributors": ["Graybar", "Wesco"],
    },
    "M.C. Dean": {
        "type": "Mission Critical Electrical",
        "strength": 98,
        "preferred_brands": ["DEWALT", "Hilti"],
        "distributors": ["Graybar", "Border States"],
    },
    "Dynaelectric": {
        "type": "Electrical Contractor",
        "strength": 82,
        "preferred_brands": ["DEWALT"],
        "distributors": ["Graybar"],
    },
    "Cupertino Electric": {
        "type": "Mission Critical Electrical",
        "strength": 93,
        "preferred_brands": ["Milwaukee", "DEWALT"],
        "distributors": ["Wesco", "Graybar"],
    },
}


def detect_contractor_ecosystem(project_row, relationships_df):
    project_text = normalize_text(" ".join([
        clean_value(project_row.get("canonical_project_name"), ""),
        clean_value(project_row.get("project_name"), ""),
        clean_value(project_row.get("permit_description"), ""),
        clean_value(project_row.get("raw_text"), ""),
        clean_value(project_row.get("strategic_notes"), ""),
    ]))

    detected = []

    for contractor, data in CONTRACTOR_ECOSYSTEM.items():

        contractor_text = normalize_text(contractor)

        found = False

        if contractor_text in project_text:
            found = True

        if not relationships_df.empty:

            contractor_matches = relationships_df[
                relationships_df["canonical_company"].astype(str).str.lower()
                == contractor.lower()
            ]

            if len(contractor_matches) >= 1:
                found = True

        if found:
            detected.append({
                "contractor": contractor,
                "type": data["type"],
                "strength": data["strength"],
                "preferred_brands": ", ".join(data["preferred_brands"]),
                "distributors": ", ".join(data["distributors"]),
            })

    return pd.DataFrame(detected)


def labor_intensity_estimate(project_row):

    mw = safe_number(project_row.get("estimated_power_mw"), 0)

    if mw >= 500:
        return "Hyperscale Workforce"

    if mw >= 250:
        return "Heavy Mission Critical Workforce"

    if mw >= 100:
        return "Large Workforce"

    if mw >= 40:
        return "Moderate Workforce"

    return "Specialized Workforce"


def procurement_stage(project_row):

    stage = normalize_text(project_row.get("project_stage"))

    if any(x in stage for x in ["concept", "planning", "rezoning"]):
        return "Pre-Procurement"

    if any(x in stage for x in ["review", "submitted", "approval"]):
        return "Contractor Positioning"

    if any(x in stage for x in ["construction", "grading", "site work"]):
        return "Field Procurement Active"

    if any(x in stage for x in ["fit out", "commissioning"]):
        return "MEP + Tool Deployment"

    return "Monitoring"


def dewalt_opportunity_score(project_row, contractor_df):

    score = 0

    mw = safe_number(project_row.get("estimated_power_mw"), 0)
    capture = safe_number(project_row.get("early_capture_score"), 0)

    score += min(mw / 10, 40)
    score += min(capture / 2, 40)

    if not contractor_df.empty:
        score += 20

    return int(min(score, 100))


def likely_distributors(contractor_df):

    if contractor_df.empty:
        return "Unknown"

    distributors = set()

    for _, row in contractor_df.iterrows():

        for d in str(row["distributors"]).split(","):
            distributors.add(d.strip())

    return ", ".join(sorted(distributors))


def tool_demand_profile(project_row):

    infrastructure = normalize_text(project_row.get("infrastructure_type"))
    mw = safe_number(project_row.get("estimated_power_mw"), 0)

    if "data center" in infrastructure:

        if mw >= 250:
            return "Hyperscale Tool Deployment"

        if mw >= 100:
            return "Heavy Mission Critical Tool Demand"

    return "Standard Infrastructure Tool Demand"
    rels, source = recover_relationships_for_project(project_row, relationships_df)

    if rels.empty:
        return "No Path Yet", 0, source

    roles = set(rels.get("relationship_role", pd.Series(dtype=str)).dropna().astype(str).tolist())

    if len(rels) >= 5 and len(roles) >= 3:
        return "Strategic Relationship Path", 90, source
    if len(rels) >= 3:
        return "Functional Relationship Path", 70, source
    if len(rels) >= 1:
        return "Basic Relationship Path", 35, source

    return "No Path Yet", 0, source


def add_relationship_counts_for_subset(df, relationships_df):
    if df.empty:
        return df

    working_df = df.copy()
    counts, sources, coverage_scores, coverage_statuses, missing_roles_list, path_statuses = [], [], [], [], [], []

    for _, row in working_df.iterrows():
        rels, source = recover_relationships_for_project(row, relationships_df)
        _, coverage_score, missing_roles, coverage_status = build_coverage_summary(rels)
        path_status, path_score, path_source = relationship_path_status(row, relationships_df)
        coverage_score = max(coverage_score, path_score)

        counts.append(len(rels))
        sources.append(source)
        coverage_scores.append(coverage_score)
        coverage_statuses.append(coverage_status)
        missing_roles_list.append(missing_roles)
        path_statuses.append(path_status)

    working_df["relationship_count"] = counts
    working_df["relationship_source"] = sources
    working_df["coverage_score"] = coverage_scores
    working_df["coverage_status"] = coverage_statuses
    working_df["missing_roles"] = missing_roles_list
    working_df["relationship_path_status"] = path_statuses

    return working_df


def build_ribbon_df(projects_df, relationships_df):
    if projects_df.empty:
        return pd.DataFrame()

    ribbon_df = projects_df.copy().head(60)
    ribbon_df = add_relationship_counts_for_subset(ribbon_df, relationships_df)
    ribbon_df["estimated_power_mw"] = pd.to_numeric(ribbon_df.get("estimated_power_mw", 0), errors="coerce").fillna(0)
    ribbon_df["early_capture_score"] = pd.to_numeric(ribbon_df["early_capture_score"], errors="coerce").fillna(0)

    ribbon_df["opportunity_status"] = ribbon_df.apply(
        lambda r: opportunity_status(
            r.get("early_capture_score"),
            r.get("estimated_power_mw"),
            r.get("relationship_count"),
            r.get("coverage_score"),
        )[0],
        axis=1,
    )

    ribbon_df["status_color"] = ribbon_df.apply(
        lambda r: opportunity_status(
            r.get("early_capture_score"),
            r.get("estimated_power_mw"),
            r.get("relationship_count"),
            r.get("coverage_score"),
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
        by=["priority_rank", "early_capture_score", "coverage_score", "relationship_count"],
        ascending=[True, False, False, False],
    )


def build_gap_report(projects_df, relationships_df):
    rows = []
    for _, row in projects_df.iterrows():
        name = clean_value(row.get("canonical_project_name"), "")
        rels, source = recover_relationships_for_project(row, relationships_df)
        _, coverage_score, missing_roles, coverage_status = build_coverage_summary(rels)
        path_status, path_score, _ = relationship_path_status(row, relationships_df)

        rows.append(
            {
                "canonical_project_name": name,
                "relationship_count": len(rels),
                "coverage_score": max(coverage_score, path_score),
                "coverage_status": coverage_status,
                "relationship_path_status": path_status,
                "missing_roles": missing_roles,
                "relationship_source": source,
            }
        )

    return pd.DataFrame(rows)


def build_account_profiles(projects_df, relationships_df):
    if projects_df.empty:
        return pd.DataFrame()

    df = projects_df.copy().head(1500)
    df["account_name"] = df.apply(primary_account, axis=1)
    df["estimated_power_mw"] = pd.to_numeric(df.get("estimated_power_mw", 0), errors="coerce").fillna(0)
    df["early_capture_score"] = pd.to_numeric(df.get("early_capture_score", 0), errors="coerce").fillna(0)

    profiles = (
        df.groupby("account_name")
        .agg(
            total_projects=("canonical_project_name", "count"),
            avg_capture_score=("early_capture_score", "mean"),
            max_capture_score=("early_capture_score", "max"),
            total_mw=("estimated_power_mw", "sum"),
            prime_projects=("capture_stage", lambda x: (x == "Prime Positioning").sum()),
            strategic_projects=("capture_stage", lambda x: (x == "Strategic Development").sum()),
        )
        .reset_index()
    )

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

    profiles["relationship_inventory"] = pd.to_numeric(profiles["relationship_inventory"], errors="coerce").fillna(0).astype(int)

    for role in COVERAGE_ROLES:
        if role not in profiles.columns:
            profiles[role] = 0
        profiles[role] = pd.to_numeric(profiles[role], errors="coerce").fillna(0).astype(int)

    profiles["coverage_roles_filled"] = profiles[COVERAGE_ROLES].apply(lambda r: sum(1 for x in r if x > 0), axis=1)
    profiles["coverage_score"] = ((profiles["coverage_roles_filled"] / len(COVERAGE_ROLES)) * 100).round(0).astype(int)
    profiles["missing_roles"] = profiles.apply(lambda r: ", ".join([role for role in COVERAGE_ROLES if r[role] == 0]) or "None", axis=1)
    profiles["total_mw"] = profiles["total_mw"].fillna(0)
    profiles["avg_capture_score"] = profiles["avg_capture_score"].round(1)
    profiles["relationship_coverage_ratio"] = (profiles["relationship_inventory"] / profiles["total_projects"]).round(2)

    profiles["account_risk"] = profiles.apply(
        lambda r: "Critical" if r["max_capture_score"] >= 90 and (r["relationship_coverage_ratio"] < 3 or r["coverage_score"] < 45)
        else "Elevated" if r["max_capture_score"] >= 75 and (r["relationship_coverage_ratio"] < 5 or r["coverage_score"] < 60)
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
            utility_dependency = clean_value(row.get("utility_dependency"), "")
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
                    "Recommended Action": "Assign BD owner and expand executive contact coverage immediately.",
                })

            if mw >= mw_threshold and coverage_score < 60:
                alerts.append({
                    "Priority": "Elevated",
                    "Alert Type": "High MW Coverage Risk",
                    "Subject": project_name,
                    "Signal": f"{int(mw)} MW opportunity with {int(coverage_score)}% coverage.",
                    "Recommended Action": "Prioritize utility, construction, and engineering stakeholder mapping.",
                })

            if utility_dependency not in ["", "N/A", "Unknown"] and "Utility" in missing_roles:
                alerts.append({
                    "Priority": "Elevated",
                    "Alert Type": "Utility Path Missing",
                    "Subject": project_name,
                    "Signal": f"Utility dependency detected: {utility_dependency}.",
                    "Recommended Action": "Map strategic accounts, transmission, and large-load contacts.",
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
                    "Recommended Action": f"Strengthen account coverage lanes: {missing}.",
                })

    alert_df = pd.DataFrame(alerts)

    if alert_df.empty:
        return pd.DataFrame(columns=["Priority", "Alert Type", "Subject", "Signal", "Recommended Action"])

    priority_order = {"Critical": 1, "Elevated": 2, "Opportunity": 3, "Monitor": 4}
    alert_df["priority_rank"] = alert_df["Priority"].map(priority_order).fillna(9)

    return alert_df.sort_values(["priority_rank", "Alert Type", "Subject"]).drop(columns=["priority_rank"])


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
        height: 292px;
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
st.caption("Allen Hammett AI — Executive Infrastructure Intelligence + Relationship Mapping Truth Layer")

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

    active_score = int(safe_number(active_row.get("early_capture_score"), 0))
    active_mw = safe_number(active_row.get("estimated_power_mw"), 0)
    active_relationships = len(active_rels)
    active_threat = threat_level(active_score, active_mw, active_relationships, active_coverage_score)
    active_readiness = deal_readiness(active_score, active_relationships, active_mw, active_coverage_score)

    st.markdown(
        f"""
        <div class="active-strip">
            <div class="active-strip-title">Active Opportunity</div>
            <div class="active-strip-name">{active_project_name}</div>
            <div class="active-strip-meta">
                Score {active_score} • Readiness {int(active_readiness)}% • Threat {active_threat} • Relationships {active_relationships} • Coverage {active_coverage_score}% • Path {active_path_status} • Missing {active_missing_roles} • MW {int(active_mw) if active_mw > 0 else "N/A"}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

if st.session_state.ribbon_message:
    st.success(st.session_state.ribbon_message)


main_tab, alerts_tab, operations_tab, deal_tab, relationships_tab, accounts_tab, analytics_tab, exports_tab = st.tabs(
    [
        "Command Wall",
        "Executive Alerts",
        "Opportunity Operations",
        "Deal Control",
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
                    MW: {estimated_power_mw}<br/>
                    Relationships: {relationship_count}<br/>
                    Coverage: {coverage_score}%<br/>
                    Path: {relationship_path_status}<br/>
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
        priority_cols = ["canonical_project_name", "early_capture_score", "capture_stage"]
        available_priority_cols = [c for c in priority_cols if c in filtered_df.columns]

        if not filtered_df.empty:
            st.dataframe(filtered_df[available_priority_cols].head(10), use_container_width=True, height=300)
        else:
            st.info("No priority records available.")


with alerts_tab:
    st.markdown("## Executive Alert Feed")

    if executive_alerts_df.empty:
        st.success("No active executive alerts under current thresholds.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Alerts", len(executive_alerts_df))
        c2.metric("Critical", len(executive_alerts_df[executive_alerts_df["Priority"] == "Critical"]))
        c3.metric("Elevated", len(executive_alerts_df[executive_alerts_df["Priority"] == "Elevated"]))

        alert_filter = st.selectbox(
            "Alert Priority Filter",
            ["All", "Critical", "Elevated", "Opportunity"],
            index=0,
            key="alert_priority_filter",
        )

        display_alerts = executive_alerts_df.copy()

        if alert_filter != "All":
            display_alerts = display_alerts[display_alerts["Priority"] == alert_filter]

        st.dataframe(display_alerts, use_container_width=True, height=500)

        st.markdown("### BD Next-Move Queue")
        for _, alert in display_alerts.head(8).iterrows():
            priority = clean_value(alert.get("Priority"))
            subject = clean_value(alert.get("Subject"))
            action = clean_value(alert.get("Recommended Action"))
            if priority == "Critical":
                st.error(f"{subject}: {action}")
            elif priority == "Elevated":
                st.warning(f"{subject}: {action}")
            else:
                st.info(f"{subject}: {action}")


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
                coverage_score = clean_value(project.get("coverage_score"), "0")
                path_status = clean_value(project.get("relationship_path_status"), "No Path Yet")
                project_id = clean_value(project.get("id"), f"row_{idx}")

                if st.button(f"{project_name} | Score {score} | Rel {rel_count} | Cov {coverage_score}% | {path_status}", use_container_width=True, key=f"project_select_{project_id}_{idx}"):
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
                coverage_summary, coverage_score, missing_roles, coverage_status = build_coverage_summary(project_relationships)
                path_status, path_score, _ = relationship_path_status(row, relationships_df)
                coverage_score = max(coverage_score, path_score)

                st.markdown(f"# {selected_project}")

                s1, s2, s3, s4, s5, s6 = st.columns(6)
                s1.metric("Score", clean_value(row.get("early_capture_score")))
                s2.metric("Stage", clean_value(row.get("capture_stage")))
                s3.metric("MW", clean_value(row.get("estimated_power_mw")))
                s4.metric("Relationships", len(project_relationships))
                s5.metric("Coverage", f"{coverage_score}%")
                s6.metric("Path", path_status)

                st.caption(match_note)

                st.markdown("## AI Recommended Actions")
                for action in recommended_actions(row, len(project_relationships), coverage_score, missing_roles):
                    st.success(action)

                overview_tab, strategy_tab, coverage_tab, relationship_tab, permit_tab, raw_tab = st.tabs(
                    ["Overview", "Strategy", "Coverage", "Relationships", "Permit", "Raw Intel"]
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

                    st.markdown("### Infrastructure Risk Flags")
                    risk_flags = clean_value(row.get("risk_flags"), "")
                    if risk_flags == "":
                        st.success("No critical infrastructure risks detected.")
                    else:
                        for risk in str(risk_flags).split(","):
                            if risk.strip():
                                st.warning(risk.strip())

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
                        relationship_columns = [
                            "full_name", "title", "company", "canonical_company", "relationship_role", "email", "phone_number",
                            "linkedin_url", "influence_score", "influence_tier", "match_type", "match_strength",
                        ]
                        existing_cols = [c for c in relationship_columns if c in project_relationships.columns]
                        st.dataframe(project_relationships[existing_cols], use_container_width=True, height=450)
                        st.markdown("## Contractor Ecosystem Intelligence")

contractor_df = detect_contractor_ecosystem(row, relationships_df)

if contractor_df.empty:

    st.warning("No contractor ecosystem identified yet.")

else:

    st.dataframe(contractor_df, use_container_width=True)

    st.success(
        f"""
Labor Scale: {labor_intensity_estimate(row)}

Procurement Stage: {procurement_stage(row)}

DEWALT Opportunity Score:
{dewalt_opportunity_score(row, contractor_df)}/100

Likely Distributor Ecosystem:
{likely_distributors(contractor_df)}

Tool Demand Profile:
{tool_demand_profile(row)}
"""
    )

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
            coverage_summary, coverage_score, missing_roles, coverage_status = build_coverage_summary(project_relationships)
            path_status, path_score, _ = relationship_path_status(row, relationships_df)
            coverage_score = max(coverage_score, path_score)

            relationship_count = len(project_relationships)
            score = safe_number(row.get("early_capture_score"), 0)
            mw = safe_number(row.get("estimated_power_mw"), 0)
            threat = threat_level(score, mw, relationship_count, coverage_score)
            readiness = deal_readiness(score, relationship_count, mw, coverage_score)

            st.markdown(f"### {selected_project}")

            d1, d2, d3, d4, d5, d6 = st.columns(6)
            d1.metric("Capture Score", int(score))
            d2.metric("Deal Readiness", f"{int(readiness)}%")
            d3.metric("MW Opportunity", int(mw) if mw > 0 else "N/A")
            d4.metric("Relationships", relationship_count)
            d5.metric("Coverage", f"{coverage_score}%")
            d6.metric("Threat Level", threat)

            st.caption(match_note)
            st.info(f"Relationship Path Status: {path_status}")

            st.markdown("---")

            control_left, control_right = st.columns([1.5, 1])

            with control_left:
                st.markdown("### Recommended BD Moves")
                for action in recommended_actions(row, relationship_count, coverage_score, missing_roles):
                    st.success(action)

                st.markdown("### Deal Control Summary")
                st.markdown(f"**Capture Stage:** {clean_value(row.get('capture_stage'))}")
                st.markdown(f"**Project Stage:** {clean_value(row.get('project_stage'))}")
                st.markdown(f"**Infrastructure Type:** {clean_value(row.get('infrastructure_type'))}")
                st.markdown(f"**Market Cluster:** {clean_value(row.get('market_cluster'))}")
                st.markdown(f"**County:** {clean_value(row.get('county'))}")
                st.markdown(f"**Utility Dependency:** {clean_value(row.get('utility_dependency'))}")
                st.markdown(f"**Missing Coverage Lanes:** {missing_roles}")

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
                elif coverage_score < 45:
                    st.error("Relationship lanes are critically incomplete.")
                elif coverage_score < 75:
                    st.warning("Moderate relationship coverage. Fill missing lanes.")
                else:
                    st.success("Strong relationship coverage.")

            st.markdown("---")
            st.markdown("### Coverage Matrix")
            coverage_df = pd.DataFrame(
                [{"Coverage Lane": role, "Contacts": count, "Covered": "Yes" if count > 0 else "No"} for role, count in coverage_summary.items()]
            )
            st.dataframe(coverage_df, use_container_width=True, height=300)

            st.markdown("### Executive Influence Ranking")

            if project_relationships.empty:
                st.warning("No executive relationships mapped to this opportunity.")
            else:
                influence_cols = [
                    "full_name", "title", "company", "canonical_company", "relationship_role", "email", "phone_number",
                    "linkedin_url", "influence_score", "influence_tier", "match_type", "match_strength",
                ]
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

    relationship_columns = [
        "canonical_project_name", "company", "canonical_company", "full_name", "title",
        "relationship_role", "email", "phone_number", "linkedin_url", "city", "state",
        "influence_score", "influence_tier", "research_status",
    ]
    existing_relationship_cols = [c for c in relationship_columns if c in display_relationships.columns]

    if display_relationships.empty:
        st.info("No executive relationships found.")
    else:
        st.dataframe(display_relationships[existing_relationship_cols], use_container_width=True, height=650)


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

        selected_account = st.selectbox(
            "Select Strategic Account",
            top_accounts["account_name"].tolist(),
            index=0,
            key="selected_account_select",
        )

        st.session_state.selected_account = selected_account

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
            st.warning(f"Account requires BD attention. Missing lanes: {selected_profile['missing_roles']}")
        else:
            st.success("Account is in monitoring posture.")

        st.markdown("### Account Coverage Matrix")
        account_coverage_df = pd.DataFrame(
            [{"Coverage Lane": role, "Contacts": int(selected_profile[role]), "Covered": "Yes" if int(selected_profile[role]) > 0 else "No"} for role in COVERAGE_ROLES]
        )
        st.dataframe(account_coverage_df, use_container_width=True, height=300)

        account_project_df = filtered_df.copy()
        if not account_project_df.empty:
            account_project_df["account_name"] = account_project_df.apply(primary_account, axis=1)
            account_project_df = account_project_df[account_project_df["account_name"] == selected_account]

            st.markdown("### Account Project Portfolio")
            account_cols = [
                "canonical_project_name",
                "capture_stage",
                "early_capture_score",
                "estimated_power_mw",
                "infrastructure_type",
                "project_stage",
                "county",
                "market_cluster",
            ]
            available_account_cols = [c for c in account_cols if c in account_project_df.columns]
            st.dataframe(account_project_df[available_account_cols].head(50), use_container_width=True, height=360)

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
        st.markdown("### Alert Priority Distribution")
        if not executive_alerts_df.empty:
            alert_chart = executive_alerts_df["Priority"].value_counts().reset_index()
            alert_chart.columns = ["Priority", "Alerts"]
            fig = px.bar(alert_chart, x="Priority", y="Alerts", template="plotly_dark", color="Priority")
            fig.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No alert distribution available.")

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
        st.markdown("### Relationship Role Distribution")
        if not relationships_df.empty and "relationship_role" in relationships_df.columns:
            role_chart = relationships_df["relationship_role"].value_counts().reset_index()
            role_chart.columns = ["Role", "Contacts"]
            fig = px.bar(role_chart, x="Role", y="Contacts", template="plotly_dark", color="Role")
            fig.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


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

    if not account_profiles_global.empty:
        st.download_button(
            "Download Strategic Account Intelligence CSV",
            account_profiles_global.to_csv(index=False),
            "strategic_account_intelligence.csv",
            "text/csv",
            key="download_account_intelligence_csv",
        )

    if not executive_alerts_df.empty:
        st.download_button(
            "Download Executive Alert Feed CSV",
            executive_alerts_df.to_csv(index=False),
            "executive_alert_feed.csv",
            "text/csv",
            key="download_executive_alert_feed_csv",
        )

    if not st.session_state.gap_report_df.empty:
        st.download_button(
            "Download Relationship Coverage Gap Report",
            st.session_state.gap_report_df.to_csv(index=False),
            "relationship_coverage_gap_report.csv",
            "text/csv",
            key="download_relationship_gap_report",
        )

    if st.button("Sign Out", key="sign_out_button"):
        st.session_state.user = None
        st.rerun()


st.caption("Allen Hammett AI • Institutional Infrastructure Intelligence Operating System")
