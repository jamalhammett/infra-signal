# app.py
# Phase 11B — DEWALT / Stanley Black & Decker Capture Intelligence OS

import os
import re
import pandas as pd
import streamlit as st
import psycopg2
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
    value = str(value).strip()
    if value.lower() in ["", "nan", "none", "null"]:
        return fallback
    return value


def safe_number(value, default=0):
    try:
        value = pd.to_numeric(value, errors="coerce")
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def normalize_text(value):
    text = clean_value(value, "").lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def canonical_company(value):
    text = normalize_text(value)

    aliases = {
        "Aligned Data Centers": ["aligned", "aligned data centers", "aligneddc"],
        "Vantage": ["vantage", "vantage data centers"],
        "QTS": ["qts", "qts data centers"],
        "STACK Infrastructure": ["stack", "stack infrastructure"],
        "Digital Realty": ["digital realty"],
        "Equinix": ["equinix"],
        "CyrusOne": ["cyrusone", "cyrus one"],
        "Dominion Energy": ["dominion", "dominion energy"],
        "NOVEC": ["novec"],
        "DPR Construction": ["dpr", "dpr construction"],
        "Turner Construction": ["turner", "turner construction"],
        "HITT": ["hitt", "hitt contracting"],
        "Clayco": ["clayco"],
        "Whiting-Turner": ["whiting turner", "whiting-turner"],
        "Holder Construction": ["holder", "holder construction"],
        "JE Dunn": ["je dunn"],
        "Fortis Construction": ["fortis"],
        "Rosendin": ["rosendin"],
        "M.C. Dean": ["m.c. dean", "mc dean", "m c dean"],
        "Dynaelectric": ["dynaelectric"],
        "Cupertino Electric": ["cupertino electric"],
        "Graybar": ["graybar"],
        "Wesco": ["wesco"],
        "White Cap": ["white cap"],
        "Fastenal": ["fastenal"],
        "Grainger": ["grainger"],
    }

    for company, keys in aliases.items():
        if any(k in text for k in keys):
            return company

    return clean_value(value, "Unknown")


def classify_relationship_role(title, company=""):
    t = normalize_text(title)
    c = normalize_text(company)

    if any(x in c for x in ["dominion", "novec", "utility", "electric"]):
        return "Utility"
    if any(x in c for x in ["dpr", "turner", "hitt", "clayco", "holder", "fortis"]):
        return "Construction"
    if any(x in c for x in ["rosendin", "dean", "dynaelectric", "cupertino"]):
        return "Electrical / MEP"
    if any(x in t for x in ["utility", "power", "transmission", "electric"]):
        return "Utility"
    if any(x in t for x in ["construction", "superintendent", "mission critical"]):
        return "Construction"
    if any(x in t for x in ["engineering", "engineer", "design"]):
        return "Engineering"
    if any(x in t for x in ["procurement", "sourcing", "supply", "vendor"]):
        return "Procurement"
    if any(x in t for x in ["operations", "facility", "facilities"]):
        return "Operations"
    if any(x in t for x in ["development", "real estate", "strategic accounts"]):
        return "Owner / Developer"
    if any(x in t for x in ["county", "planning", "zoning", "government"]):
        return "Government"
    return "General Executive"


def influence_score(title):
    title = str(title or "").lower()
    score = 0
    if any(x in title for x in ["chief", "ceo", "president"]):
        score += 40
    if any(x in title for x in ["vice president", "vp"]):
        score += 30
    if "director" in title:
        score += 25
    if "manager" in title:
        score += 15
    if any(x in title for x in ["construction", "operations", "procurement", "engineering", "utility", "superintendent", "field"]):
        score += 25
    return score


def influence_tier(score):
    if score >= 60:
        return "High Influence"
    if score >= 35:
        return "Medium Influence"
    return "Monitor"


COVERAGE_ROLES = [
    "Owner / Developer",
    "Utility",
    "Construction",
    "Engineering",
    "Procurement",
    "Operations",
    "Government",
]


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


def authenticate(email, password):
    df = run_query(
        """
        SELECT *
        FROM users
        WHERE lower(email)=lower(%s)
          AND password=%s
        LIMIT 1
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
        UPDATE users
        SET password=%s
        WHERE lower(email)=lower(%s)
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
    st.subheader("Infrastructure Capture Intelligence Access")

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
        SELECT *
        FROM projects
        ORDER BY early_capture_score DESC NULLS LAST,
                 created_at DESC NULLS LAST
        LIMIT 5000
        """
    )

    if df.empty:
        return df

    if "early_capture_score" not in df.columns:
        df["early_capture_score"] = 0

    df["early_capture_score"] = pd.to_numeric(df["early_capture_score"], errors="coerce").fillna(0)
    df["capture_stage"] = df["early_capture_score"].apply(capture_stage)

    if "estimated_power_mw" not in df.columns:
        df["estimated_power_mw"] = 0

    df["estimated_power_mw"] = pd.to_numeric(df["estimated_power_mw"], errors="coerce").fillna(0)

    return df


@st.cache_data(ttl=300)
def load_relationships():
    try:
        df = run_query(
            """
            SELECT *
            FROM executive_project_matches
            ORDER BY final_score DESC NULLS LAST,
                     confidence DESC NULLS LAST,
                     imported_at DESC NULLS LAST
            """
        )
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    if "canonical_company" not in df.columns:
        df["canonical_company"] = df.get("company", "").apply(canonical_company)

    if "relationship_role" not in df.columns:
        df["relationship_role"] = df.apply(
            lambda r: classify_relationship_role(r.get("title", ""), r.get("company", "")),
            axis=1,
        )

    df["influence_score"] = df.get("title", "").apply(influence_score)
    df["influence_tier"] = df["influence_score"].apply(influence_tier)

    return df


def extract_project_companies(row):
    fields = [
        "canonical_project_name",
        "project_name",
        "applicant_name",
        "owner",
        "developer",
        "company_name",
        "permit_description",
        "raw_text",
        "strategic_notes",
        "utility_dependency",
    ]

    companies = set()

    combined = " ".join([normalize_text(row.get(f, "")) for f in fields])

    for company in [
        "Aligned Data Centers",
        "Vantage",
        "QTS",
        "STACK Infrastructure",
        "Digital Realty",
        "Equinix",
        "CyrusOne",
        "Dominion Energy",
        "NOVEC",
        "DPR Construction",
        "Turner Construction",
        "HITT",
        "Clayco",
        "Whiting-Turner",
        "Holder Construction",
        "Rosendin",
        "M.C. Dean",
    ]:
        if normalize_text(company) in combined:
            companies.add(company)

    return companies


def recover_relationships_for_project(project_row, relationships_df):
    if relationships_df.empty:
        return pd.DataFrame(), "No relationship data loaded"

    project_name = clean_value(project_row.get("canonical_project_name"), "")
    project_text = normalize_text(" ".join([
        clean_value(project_row.get("canonical_project_name"), ""),
        clean_value(project_row.get("project_name"), ""),
        clean_value(project_row.get("applicant_name"), ""),
        clean_value(project_row.get("company_name"), ""),
        clean_value(project_row.get("permit_description"), ""),
        clean_value(project_row.get("raw_text"), ""),
        clean_value(project_row.get("strategic_notes"), ""),
    ]))

    rel_df = relationships_df.copy()

    for col in ["canonical_project_name", "company", "canonical_company", "relationship_role", "final_score", "confidence"]:
        if col not in rel_df.columns:
            rel_df[col] = ""

    rel_df["match_strength"] = 0
    rel_df["match_type"] = "Unmapped"

    direct_mask = rel_df["canonical_project_name"].astype(str).str.lower() == project_name.lower()
    rel_df.loc[direct_mask, "match_strength"] = 100
    rel_df.loc[direct_mask, "match_type"] = "Direct Project Match"

    project_companies = extract_project_companies(project_row)

    for company in project_companies:
        mask = rel_df["canonical_company"].astype(str).str.lower() == company.lower()
        rel_df.loc[mask & (rel_df["match_strength"] < 90), "match_strength"] = 90
        rel_df.loc[mask & (rel_df["match_type"] == "Unmapped"), "match_type"] = "Strategic Account Match"

    for idx, rel in rel_df.iterrows():
        company = normalize_text(rel.get("canonical_company") or rel.get("company"))
        if company and company in project_text and rel_df.at[idx, "match_strength"] < 80:
            rel_df.at[idx, "match_strength"] = 80
            rel_df.at[idx, "match_type"] = "Company Text Match"

    recovered = rel_df[rel_df["match_strength"] > 0].copy()

    if recovered.empty:
        return pd.DataFrame(), "No relationship path identified"

    recovered["final_score"] = pd.to_numeric(recovered["final_score"], errors="coerce").fillna(0)
    recovered["confidence"] = pd.to_numeric(recovered["confidence"], errors="coerce").fillna(0)

    recovered = recovered.sort_values(
        by=["match_strength", "final_score", "confidence"],
        ascending=False,
    )

    return recovered, f"Relationship path identified: {recovered.iloc[0]['match_type']}"


def build_coverage_summary(relationships):
    summary = {role: 0 for role in COVERAGE_ROLES}

    if relationships is None or relationships.empty:
        return summary, 0, ", ".join(COVERAGE_ROLES), "Critical"

    for role in COVERAGE_ROLES:
        summary[role] = int((relationships["relationship_role"].astype(str) == role).sum())

    covered = sum(1 for count in summary.values() if count > 0)
    score = int(round((covered / len(COVERAGE_ROLES)) * 100))
    missing = [role for role, count in summary.items() if count == 0]

    if score >= 75:
        status = "Strong"
    elif score >= 45:
        status = "Moderate"
    elif score >= 20:
        status = "Weak"
    else:
        status = "Critical"

    return summary, score, ", ".join(missing) if missing else "None", status


def relationship_path_status(project_row, relationships_df):
    rels, source = recover_relationships_for_project(project_row, relationships_df)

    if rels.empty:
        return "No Path Yet", 0, source

    roles = set(rels["relationship_role"].dropna().astype(str).tolist())

    if len(rels) >= 5 and len(roles) >= 3:
        return "Strategic Relationship Path", 90, source
    if len(rels) >= 3:
        return "Functional Relationship Path", 70, source
    return "Basic Relationship Path", 35, source


CONTRACTOR_ECOSYSTEM = {
    "DPR Construction": {"type": "Prime GC", "strength": 95, "preferred_brands": ["DEWALT", "Milwaukee", "Hilti"], "distributors": ["White Cap", "Fastenal", "Grainger"]},
    "Turner Construction": {"type": "Prime GC", "strength": 94, "preferred_brands": ["DEWALT", "Milwaukee"], "distributors": ["White Cap", "HD Supply", "Grainger"]},
    "HITT": {"type": "Prime GC", "strength": 88, "preferred_brands": ["DEWALT", "Milwaukee"], "distributors": ["White Cap", "Fastenal"]},
    "Clayco": {"type": "Prime GC", "strength": 86, "preferred_brands": ["DEWALT"], "distributors": ["White Cap", "Grainger"]},
    "Whiting-Turner": {"type": "Prime GC", "strength": 90, "preferred_brands": ["Milwaukee", "DEWALT"], "distributors": ["Fastenal", "Grainger"]},
    "Holder Construction": {"type": "Prime GC", "strength": 92, "preferred_brands": ["DEWALT", "Milwaukee"], "distributors": ["White Cap", "Fastenal"]},
    "Rosendin": {"type": "MEP / Electrical", "strength": 91, "preferred_brands": ["DEWALT", "Milwaukee"], "distributors": ["Graybar", "Wesco"]},
    "M.C. Dean": {"type": "Mission Critical Electrical", "strength": 98, "preferred_brands": ["DEWALT", "Hilti"], "distributors": ["Graybar", "Border States"]},
    "Dynaelectric": {"type": "Electrical Contractor", "strength": 82, "preferred_brands": ["DEWALT"], "distributors": ["Graybar"]},
    "Cupertino Electric": {"type": "Mission Critical Electrical", "strength": 93, "preferred_brands": ["Milwaukee", "DEWALT"], "distributors": ["Wesco", "Graybar"]},
}


def detect_contractor_ecosystem(project_row, relationships_df):
    text = normalize_text(" ".join([
        clean_value(project_row.get("canonical_project_name"), ""),
        clean_value(project_row.get("project_name"), ""),
        clean_value(project_row.get("permit_description"), ""),
        clean_value(project_row.get("raw_text"), ""),
        clean_value(project_row.get("strategic_notes"), ""),
    ]))

    detected = []

    for contractor, data in CONTRACTOR_ECOSYSTEM.items():
        found = normalize_text(contractor) in text

        if not relationships_df.empty and "canonical_company" in relationships_df.columns:
            if len(relationships_df[relationships_df["canonical_company"].astype(str).str.lower() == contractor.lower()]) > 0:
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


def labor_intensity_estimate(row):
    mw = safe_number(row.get("estimated_power_mw"), 0)
    if mw >= 500:
        return "Hyperscale Workforce"
    if mw >= 250:
        return "Heavy Mission Critical Workforce"
    if mw >= 100:
        return "Large Workforce"
    if mw >= 40:
        return "Moderate Workforce"
    return "Specialized Workforce"


def procurement_stage(row):
    stage = normalize_text(row.get("project_stage"))

    if any(x in stage for x in ["concept", "planning", "rezoning"]):
        return "Pre-Procurement"
    if any(x in stage for x in ["review", "submitted", "approval"]):
        return "Contractor Positioning"
    if any(x in stage for x in ["construction", "grading", "site work"]):
        return "Field Procurement Active"
    if any(x in stage for x in ["fit out", "commissioning"]):
        return "MEP + Tool Deployment"
    return "Monitoring"


def dewalt_opportunity_score(row, contractor_df):
    score = min(safe_number(row.get("estimated_power_mw"), 0) / 10, 40)
    score += min(safe_number(row.get("early_capture_score"), 0) / 2, 40)

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


def tool_demand_profile(row):
    infra = normalize_text(row.get("infrastructure_type"))
    mw = safe_number(row.get("estimated_power_mw"), 0)

    if "data center" in infra:
        if mw >= 250:
            return "Hyperscale Tool Deployment"
        if mw >= 100:
            return "Heavy Mission Critical Tool Demand"

    return "Standard Infrastructure Tool Demand"


def infer_project_phase(name):
    name = normalize_text(name)

    if any(x in name for x in ["rezoning", "site plan", "permit", "application"]):
        return "Pre-Construction"
    if any(x in name for x in ["substation", "utility", "transmission", "power"]):
        return "Power + Utility Infrastructure"
    if any(x in name for x in ["grading", "civil", "sitework", "earthwork"]):
        return "Site Development"
    if any(x in name for x in ["steel", "shell", "structure"]):
        return "Structural Buildout"
    if any(x in name for x in ["fit out", "commissioning"]):
        return "Commissioning"
    return "Core Construction"


def next_buying_window(phase):
    mapping = {
        "Pre-Construction": "Specification + relationship positioning",
        "Power + Utility Infrastructure": "Electrical contractor engagement",
        "Site Development": "Civil + concrete procurement",
        "Structural Buildout": "Steel + fastening + anchor systems",
        "Core Construction": "High-volume cordless + jobsite deployment",
        "Commissioning": "Operations + maintenance conversion",
    }
    return mapping.get(phase, "General contractor engagement")


def dewalt_product_push(phase):
    mapping = {
        "Pre-Construction": "Lasers, layout tools, measuring, jobsite planning",
        "Power + Utility Infrastructure": "Electrical tools, storage, bandsaws, knockout tools",
        "Site Development": "Concrete anchors, outdoor power, generators, storage",
        "Structural Buildout": "Fastening, drills, impacts, anchors, grinders",
        "Core Construction": "20V MAX, FLEXVOLT, storage, safety, consumables",
        "Commissioning": "Maintenance kits, diagnostics, service tools",
    }
    return mapping.get(phase, "General DEWALT deployment")


def sales_action(score, relationships):
    if score >= 90 and relationships >= 10:
        return "EXECUTIVE PURSUIT"
    if score >= 75:
        return "CONTRACTOR ENGAGEMENT"
    if relationships >= 5:
        return "EXPAND ACCOUNT PENETRATION"
    return "BUILD RELATIONSHIP COVERAGE"


def add_relationship_counts(df, relationships_df):
    if df.empty:
        return df

    out = df.copy()
    counts, coverage_scores, path_statuses = [], [], []

    for _, row in out.iterrows():
        rels, _ = recover_relationships_for_project(row, relationships_df)
        _, coverage, _, _ = build_coverage_summary(rels)
        path, path_score, _ = relationship_path_status(row, relationships_df)

        counts.append(len(rels))
        coverage_scores.append(max(coverage, path_score))
        path_statuses.append(path)

    out["relationship_count"] = counts
    out["coverage_score"] = coverage_scores
    out["relationship_path_status"] = path_statuses

    return out


def build_account_profiles(projects_df, relationships_df):
    if projects_df.empty:
        return pd.DataFrame()

    df = projects_df.copy()
    df["account_name"] = df.apply(lambda r: list(extract_project_companies(r))[0] if extract_project_companies(r) else "Unknown Account", axis=1)

    profiles = (
        df.groupby("account_name")
        .agg(
            total_projects=("canonical_project_name", "count"),
            max_capture_score=("early_capture_score", "max"),
            avg_capture_score=("early_capture_score", "mean"),
            total_mw=("estimated_power_mw", "sum"),
        )
        .reset_index()
    )

    if not relationships_df.empty:
        rel_counts = (
            relationships_df.groupby("canonical_company")
            .size()
            .reset_index(name="relationship_inventory")
            .rename(columns={"canonical_company": "account_name"})
        )
        profiles = profiles.merge(rel_counts, on="account_name", how="left")
    else:
        profiles["relationship_inventory"] = 0

    profiles["relationship_inventory"] = pd.to_numeric(profiles["relationship_inventory"], errors="coerce").fillna(0).astype(int)
    profiles["coverage_score"] = profiles["relationship_inventory"].apply(lambda x: min(int(x * 10), 100))
    profiles["account_risk"] = profiles.apply(
        lambda r: "Critical" if r["max_capture_score"] >= 90 and r["relationship_inventory"] < 3 else "Elevated" if r["max_capture_score"] >= 75 else "Monitor",
        axis=1,
    )

    return profiles.sort_values("max_capture_score", ascending=False)


def build_dewalt_intelligence_layer(account_profiles_df):
    if account_profiles_df is None or account_profiles_df.empty:
        return pd.DataFrame()

    rows = []

    for _, row in account_profiles_df.iterrows():
        account = clean_value(row.get("account_name"), "Unknown Account")
        phase = infer_project_phase(account)
        relationships = int(safe_number(row.get("relationship_inventory"), 0))
        score = int(safe_number(row.get("max_capture_score"), 0))

        rows.append({
            "account": account,
            "tracked_projects": int(safe_number(row.get("total_projects"), 0)),
            "total_mw": int(safe_number(row.get("total_mw"), 0)),
            "capture_score": score,
            "relationships": relationships,
            "coverage_score": int(safe_number(row.get("coverage_score"), 0)),
            "phase": phase,
            "buying_window": next_buying_window(phase),
            "dewalt_product_demand": dewalt_product_push(phase),
            "sales_action": sales_action(score, relationships),
        })

    return pd.DataFrame(rows)


login_gate()

projects_df = load_projects()
relationships_df = load_relationships()

if "selected_project" not in st.session_state:
    st.session_state.selected_project = projects_df.iloc[0]["canonical_project_name"] if not projects_df.empty else None

if "watchlist" not in st.session_state:
    st.session_state.watchlist = []

st.sidebar.header("Executive Filters")
search = st.sidebar.text_input("Search")
prime_only = st.sidebar.checkbox("Prime Only")
contractor_focus = st.sidebar.checkbox("Contractor / DEWALT Focus")

filtered_df = projects_df.copy()

if not filtered_df.empty:
    if search:
        filtered_df = filtered_df[
            filtered_df.astype(str).apply(lambda r: r.str.contains(search, case=False, na=False).any(), axis=1)
        ]

    if prime_only:
        filtered_df = filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]

account_profiles_global = build_account_profiles(filtered_df, relationships_df)
dewalt_df_global = build_dewalt_intelligence_layer(account_profiles_global)

st.title("Infrastructure Intelligence Operating System")
st.caption("DEWALT / Stanley Black & Decker Capture Intelligence")

tabs = st.tabs([
    "Command Wall",
    "Opportunity Operations",
    "Contractor Intelligence",
    "Relationship Command",
    "Account Intelligence",
    "Market Analytics",
    "Exports",
])

command_tab, operations_tab, contractor_tab, relationships_tab, accounts_tab, analytics_tab, exports_tab = tabs


with command_tab:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Signals", len(filtered_df))
    c2.metric("Relationships", len(relationships_df))
    c3.metric("Accounts", len(account_profiles_global))
    c4.metric("DEWALT Targets", len(dewalt_df_global[dewalt_df_global["sales_action"] == "EXECUTIVE PURSUIT"]) if not dewalt_df_global.empty else 0)
    c5.metric("Prime", len(filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]) if not filtered_df.empty else 0)

    st.markdown("## Priority Opportunities")

    display_df = add_relationship_counts(filtered_df.head(100), relationships_df)

    cols = [
        "canonical_project_name",
        "capture_stage",
        "early_capture_score",
        "estimated_power_mw",
        "relationship_count",
        "coverage_score",
        "relationship_path_status",
        "county",
        "market_cluster",
    ]

    existing_cols = [c for c in cols if c in display_df.columns]

    st.dataframe(display_df[existing_cols], use_container_width=True, height=600)


with operations_tab:
    st.markdown("## Opportunity Operations")

    if filtered_df.empty:
        st.info("No projects available.")
    else:
        project_names = filtered_df["canonical_project_name"].astype(str).tolist()

        selected = st.selectbox(
            "Select Opportunity",
            project_names,
            index=project_names.index(st.session_state.selected_project) if st.session_state.selected_project in project_names else 0,
        )

        st.session_state.selected_project = selected
        row = filtered_df[filtered_df["canonical_project_name"].astype(str) == selected].iloc[0]

        rels, match_note = recover_relationships_for_project(row, relationships_df)
        coverage_summary, coverage_score, missing_roles, coverage_status = build_coverage_summary(rels)
        path_status, path_score, _ = relationship_path_status(row, relationships_df)
        coverage_score = max(coverage_score, path_score)
        contractor_df = detect_contractor_ecosystem(row, relationships_df)

        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric("Score", int(safe_number(row.get("early_capture_score"))))
        k2.metric("MW", int(safe_number(row.get("estimated_power_mw"))))
        k3.metric("Relationships", len(rels))
        k4.metric("Coverage", f"{coverage_score}%")
        k5.metric("Path", path_status)
        k6.metric("DEWALT Score", f"{dewalt_opportunity_score(row, contractor_df)}/100")

        st.caption(match_note)

        inner = st.tabs(["Overview", "Relationships", "Contractor Ecosystem", "DEWALT Demand", "Raw Intel"])
        overview_tab, rel_tab, contractor_inner_tab, demand_inner_tab, raw_tab = inner

        with overview_tab:
            st.write(row.to_frame("Value"))

        with rel_tab:
            if rels.empty:
                st.warning("No relationships mapped.")
            else:
                cols = [
                    "full_name",
                    "title",
                    "company",
                    "canonical_company",
                    "relationship_role",
                    "email",
                    "phone_number",
                    "linkedin_url",
                    "influence_score",
                    "influence_tier",
                    "match_type",
                    "match_strength",
                ]
                existing = [c for c in cols if c in rels.columns]
                st.dataframe(rels[existing], use_container_width=True, height=500)

        with contractor_inner_tab:
            if contractor_df.empty:
                st.warning("No contractor ecosystem identified yet.")
            else:
                st.dataframe(contractor_df, use_container_width=True)

            st.success(
                f"""
Labor Scale: {labor_intensity_estimate(row)}

Procurement Stage: {procurement_stage(row)}

Likely Distributor Ecosystem: {likely_distributors(contractor_df)}

Tool Demand Profile: {tool_demand_profile(row)}
"""
            )

        with demand_inner_tab:
            phase = infer_project_phase(selected)

            demand_row = pd.DataFrame([{
                "project": selected,
                "phase": phase,
                "buying_window": next_buying_window(phase),
                "dewalt_product_demand": dewalt_product_push(phase),
                "dewalt_score": dewalt_opportunity_score(row, contractor_df),
                "sales_action": sales_action(dewalt_opportunity_score(row, contractor_df), len(rels)),
            }])

            st.dataframe(demand_row, use_container_width=True)

        with raw_tab:
            st.code(clean_value(row.get("raw_text"), "No raw intelligence available.")[:10000])


with contractor_tab:
    st.markdown("## Contractor Ecosystem Intelligence")

    contractor_rows = []

    for _, row in filtered_df.head(500).iterrows():
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

        st.dataframe(contractor_intel_df.sort_values("dewalt_score", ascending=False), use_container_width=True, height=500)

    st.markdown("---")
    st.markdown("## DEWALT Strategic Demand Engine")

    if dewalt_df_global.empty:
        st.info("No DEWALT demand intelligence available yet.")
    else:
        st.dataframe(
            dewalt_df_global.sort_values(
                by=["capture_score", "relationships", "total_mw"],
                ascending=False,
            ),
            use_container_width=True,
            height=500,
        )

        st.markdown("### Executive Pursuit Targets")
        pursuit_df = dewalt_df_global[dewalt_df_global["sales_action"] == "EXECUTIVE PURSUIT"]

        if pursuit_df.empty:
            st.info("No executive pursuit targets yet.")
        else:
            st.dataframe(pursuit_df, use_container_width=True, height=300)

        st.markdown("### Construction Phase Distribution")
        phase_counts = dewalt_df_global["phase"].value_counts().reset_index()
        phase_counts.columns = ["Phase", "Accounts"]
        st.bar_chart(phase_counts.set_index("Phase"))


with relationships_tab:
    st.markdown("## Relationship Command")

    search_rel = st.text_input("Search Relationships")
    display_relationships = relationships_df.copy()

    if search_rel and not display_relationships.empty:
        display_relationships = display_relationships[
            display_relationships.astype(str).apply(lambda r: r.str.contains(search_rel, case=False, na=False).any(), axis=1)
        ]

    if display_relationships.empty:
        st.info("No relationships found.")
    else:
        cols = [
            "canonical_project_name",
            "company",
            "canonical_company",
            "full_name",
            "title",
            "relationship_role",
            "email",
            "phone_number",
            "linkedin_url",
            "influence_score",
            "influence_tier",
            "research_status",
        ]

        existing = [c for c in cols if c in display_relationships.columns]
        st.dataframe(display_relationships[existing], use_container_width=True, height=650)


with accounts_tab:
    st.markdown("## Strategic Account Intelligence")

    if account_profiles_global.empty:
        st.info("No account intelligence available.")
    else:
        st.dataframe(account_profiles_global, use_container_width=True, height=650)


with analytics_tab:
    st.markdown("## Market Analytics")

    a1, a2 = st.columns(2)

    with a1:
        if not filtered_df.empty:
            stage_counts = filtered_df["capture_stage"].value_counts().reset_index()
            stage_counts.columns = ["Stage", "Count"]
            fig = px.bar(stage_counts, x="Stage", y="Count", template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

    with a2:
        if not relationships_df.empty:
            role_counts = relationships_df["relationship_role"].value_counts().reset_index()
            role_counts.columns = ["Role", "Contacts"]
            fig = px.bar(role_counts, x="Role", y="Contacts", template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)


with exports_tab:
    st.markdown("## Exports")

    if not filtered_df.empty:
        st.download_button(
            "Download Infrastructure Intelligence CSV",
            filtered_df.to_csv(index=False),
            "infrastructure_intelligence.csv",
            "text/csv",
        )

    if not relationships_df.empty:
        st.download_button(
            "Download Relationship Pipeline CSV",
            relationships_df.to_csv(index=False),
            "executive_relationship_pipeline.csv",
            "text/csv",
        )

    if not account_profiles_global.empty:
        st.download_button(
            "Download Account Intelligence CSV",
            account_profiles_global.to_csv(index=False),
            "strategic_account_intelligence.csv",
            "text/csv",
        )

    if not dewalt_df_global.empty:
        st.download_button(
            "Download DEWALT Demand Intelligence CSV",
            dewalt_df_global.to_csv(index=False),
            "dewalt_demand_intelligence.csv",
            "text/csv",
        )

    if st.button("Sign Out"):
        st.session_state.user = None
        st.rerun()


st.caption("Allen Hammett AI • DEWALT / Stanley Black & Decker Infrastructure Capture Intelligence OS")
