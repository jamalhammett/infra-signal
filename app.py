import os
import psycopg2
import pandas as pd
import streamlit as st
import plotly.express as px

from dotenv import load_dotenv
from difflib import SequenceMatcher

# ====================================================
# ENV + DB CONNECTION
# ====================================================

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

@st.cache_resource(show_spinner=False)
def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    return conn

@st.cache_data(show_spinner=False)
def load_projects():
    conn = get_connection()
    query = """
        SELECT *
        FROM projects
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data(show_spinner=False)
def load_executive_matches():
    conn = get_connection()
    query = """
        SELECT *
        FROM executive_project_matches
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data(show_spinner=False)
def load_users():
    conn = get_connection()
    query = """
        SELECT *
        FROM users
    """
    df = pd.read_sql(query, conn)
    return df

# ====================================================
# AUTH / LOGIN
# ====================================================

def authenticate(username: str, password: str) -> bool:
    users_df = load_users()
    if "username" not in users_df.columns or "password" not in users_df.columns:
        return False
    row = users_df[
        (users_df["username"] == username) &
        (users_df["password"] == password)
    ]
    return not row.empty

def login_ui():
    st.title("Allen Hammett AI — Infrastructure Intelligence OS")
    st.subheader("Executive Access Portal")

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

    if submitted:
        if authenticate(username, password):
            st.session_state["authenticated"] = True
            st.session_state["username"] = username
            st.experimental_rerun()
        else:
            st.error("Invalid credentials. Please try again.")

# ====================================================
# PHASE 11B INTELLIGENCE LAYER (INLINE)
# ====================================================

TARGET_CONTRACTORS = [
    "DPR", "Turner", "HITT", "Clayco", "Holder",
    "Rosendin", "M.C. Dean", "Cupertino Electric",
    "Dynaelectric", "Whiting-Turner"
]

TARGET_DISTRIBUTORS = [
    "White Cap", "Graybar", "Wesco", "Grainger", "Fastenal"
]

def simple_fuzzy_match(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def recover_relationships(projects_df: pd.DataFrame, matches_df: pd.DataFrame) -> pd.DataFrame:
    project_keys = ["canonical_company", "developer", "owner", "applicant_name"]
    match_keys = ["canonical_company", "company_name", "developer", "owner"]

    recovered_rows = []

    for idx, prow in projects_df.iterrows():
        project_id = prow.get("project_id")
        if "project_id" in matches_df.columns:
            existing = matches_df[matches_df["project_id"] == project_id]
            if not existing.empty:
                continue

        candidates = matches_df.copy()
        scores = []

        for pk in project_keys:
            if pk not in projects_df.columns:
                continue
            pval = str(prow.get(pk, "")).strip()
            if not pval:
                continue
            for mk in match_keys:
                if mk not in candidates.columns:
                    continue
                scores.append(
                    candidates[mk].fillna("").apply(
                        lambda x: simple_fuzzy_match(pval, str(x))
                    )
                )

        if not scores:
            continue

        combined = scores[0]
        for s in scores[1:]:
            combined = combined.combine(s, max)

        candidates = candidates.assign(_recovery_score=combined)
        best = candidates[candidates["_recovery_score"] >= 0.8]

        if not best.empty:
            best = best.copy()
            best["recovered_for_project_id"] = project_id
            recovered_rows.append(best)

    if recovered_rows:
        recovered_df = pd.concat(recovered_rows, ignore_index=True)
    else:
        recovered_df = matches_df.iloc[0:0].copy()
        recovered_df["recovered_for_project_id"] = []

    return recovered_df

def infer_procurement_stage(project_row: pd.Series) -> str:
    text_fields = [
        str(project_row.get("permit_description", "")),
        str(project_row.get("project_phase", "")),
        str(project_row.get("status", "")),
    ]
    blob = " ".join(text_fields).lower()

    if any(k in blob for k in ["foundation", "site work", "grading"]):
        return "Early Civil / Site Prep"
    if any(k in blob for k in ["steel", "structure", "shell", "superstructure"]):
        return "Core & Shell"
    if any(k in blob for k in ["electrical rough-in", "mep rough-in", "rough-in"]):
        return "MEP Rough-In"
    if any(k in blob for k in ["commissioning", "startup", "testing"]):
        return "Commissioning"
    return "Unknown / Mixed"

def score_dewalt_demand(project_row: pd.Series, contractor_names) -> float:
    stage = infer_procurement_stage(project_row)
    stage_weight = {
        "Early Civil / Site Prep": 0.3,
        "Core & Shell": 0.6,
        "MEP Rough-In": 1.0,
        "Commissioning": 0.5,
        "Unknown / Mixed": 0.4,
    }.get(stage, 0.4)

    contractor_weight = 0.0
    for c in contractor_names:
        if any(tc.lower() in c.lower() for tc in TARGET_CONTRACTORS):
            contractor_weight += 0.3
    contractor_weight = min(contractor_weight, 1.0)

    base = 0.5
    score = base + 0.3 * stage_weight + 0.4 * contractor_weight
    return max(0.0, min(1.0, score))

def infer_distributors(contractor_names) -> list:
    inferred = set()
    for c in contractor_names:
        name = c.lower()
        if any(k in name for k in ["electric", "mep", "systems", "power"]):
            inferred.update(["Graybar", "Wesco"])
        if any(k in name for k in ["construction", "builders", "gc"]):
            inferred.update(["White Cap", "Fastenal"])
    return sorted(inferred)

def score_contractor_relationships(
    contractor_contacts: pd.DataFrame,
    relationship_col: str = "influence_score"
) -> float:
    if contractor_contacts.empty:
        return 0.0
    if relationship_col not in contractor_contacts.columns:
        return 0.0
    scores = contractor_contacts[relationship_col].clip(lower=0, upper=100) / 100.0
    return float(scores.mean())

# ====================================================
# DATA PREP + RELATIONSHIP COVERAGE
# ====================================================

@st.cache_data(show_spinner=False)
def build_project_relationship_view():
    projects_df = load_projects()
    matches_df = load_executive_matches()

    # Direct merge
    if "project_id" in projects_df.columns and "project_id" in matches_df.columns:
        merged_direct = projects_df.merge(
            matches_df,
            how="left",
            on="project_id",
            suffixes=("", "_match")
        )
    else:
        merged_direct = projects_df.copy()

    # Recovery
    recovered_matches = recover_relationships(projects_df, matches_df)

    if not recovered_matches.empty:
        merged_recovered = projects_df.merge(
            recovered_matches,
            how="left",
            left_on="project_id",
            right_on="recovered_for_project_id",
            suffixes=("", "_recovered")
        )
        merged_all = pd.concat([merged_direct, merged_recovered], ignore_index=True)
    else:
        merged_all = merged_direct.copy()

    if "contact_id" in merged_all.columns and "project_id" in merged_all.columns:
        rel_counts = merged_all.groupby("project_id")["contact_id"].nunique()
        projects_df = projects_df.merge(
            rel_counts.rename("relationship_count"),
            how="left",
            on="project_id"
        )
    else:
        projects_df["relationship_count"] = 0

    projects_df["relationship_count"] = projects_df["relationship_count"].fillna(0).astype(int)
    projects_df["relationship_status"] = projects_df["relationship_count"].apply(
        lambda x: "No Path Yet" if x == 0 else "Path Established"
    )

    return projects_df, merged_all

# ====================================================
# UI SECTIONS
# ====================================================

def render_command_wall(projects_df: pd.DataFrame):
    st.subheader("Command Wall")

    total_projects = len(projects_df)
    with_path = (projects_df["relationship_status"] == "Path Established").sum()
    no_path = (projects_df["relationship_status"] == "No Path Yet").sum()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Projects", total_projects)
    col2.metric("Projects with Path", with_path)
    col3.metric("Projects with No Path", no_path)

    if "relationship_status" in projects_df.columns:
        status_counts = projects_df["relationship_status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        fig = px.bar(status_counts, x="status", y="count", title="Relationship Coverage")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Project List")
    st.dataframe(
        projects_df[[
            c for c in projects_df.columns
            if c in ["project_id", "project_name", "canonical_company", "developer",
                     "owner", "relationship_status", "relationship_count"]
        ]],
        use_container_width=True
    )

def render_opportunity_operations(projects_df: pd.DataFrame, merged_all: pd.DataFrame):
    st.subheader("Opportunity Operations")

    if "project_name" in projects_df.columns:
        project_selection = st.selectbox(
            "Select Project",
            projects_df["project_name"].dropna().unique()
        )
        proj_row = projects_df[projects_df["project_name"] == project_selection].iloc[0]
    else:
        st.warning("No project_name column found.")
        return

    st.markdown("##### Project Snapshot")
    cols = st.columns(4)
    cols[0].metric("Project ID", proj_row.get("project_id", "N/A"))
    cols[1].metric("Relationship Status", proj_row.get("relationship_status", "N/A"))
    cols[2].metric("Relationship Count", int(proj_row.get("relationship_count", 0)))
    cols[3].metric("Developer", str(proj_row.get("developer", "N/A")))

    if "project_id" in merged_all.columns:
        proj_contacts = merged_all[merged_all["project_id"] == proj_row["project_id"]]
    else:
        proj_contacts = merged_all.iloc[0:0].copy()

    st.markdown("##### Contacts on This Project")
    if not proj_contacts.empty:
        cols_to_show = [
            c for c in proj_contacts.columns
            if c in ["contact_id", "full_name", "title", "email",
                     "linkedin_url", "relationship_role", "influence_score",
                     "canonical_company"]
        ]
        st.dataframe(proj_contacts[cols_to_show], use_container_width=True)
    else:
        st.info("No contacts currently mapped to this project.")

def render_relationship_command(projects_df: pd.DataFrame, merged_all: pd.DataFrame):
    st.subheader("Relationship Command")

    if "project_name" not in projects_df.columns:
        st.warning("No project_name column found.")
        return

    project_selection = st.selectbox(
        "Select Project",
        projects_df["project_name"].dropna().unique()
    )
    proj_row = projects_df[projects_df["project_name"] == project_selection].iloc[0]

    if "project_id" in merged_all.columns:
        proj_contacts = merged_all[merged_all["project_id"] == proj_row["project_id"]]
    else:
        proj_contacts = merged_all.iloc[0:0].copy()

    st.markdown("##### Relationship Map")
    if not proj_contacts.empty:
        cols_to_show = [
            c for c in proj_contacts.columns
            if c in ["full_name", "title", "email", "linkedin_url",
                     "relationship_role", "influence_score", "canonical_company"]
        ]
        st.dataframe(proj_contacts[cols_to_show], use_container_width=True)
    else:
        st.info("No mapped relationships yet. This may be a relationship gap.")

    contractor_contacts = proj_contacts[
        proj_contacts.get("relationship_role", "").astype(str).str.contains(
            "contractor", case=False, na=False
        )
    ]
    contractor_names = sorted(
        contractor_contacts.get("canonical_company", pd.Series(dtype=str))
        .dropna()
        .unique()
        .tolist()
    )

    st.markdown("### DEWALT Contractor Ecosystem Intelligence")

    procurement_stage = infer_procurement_stage(proj_row)
    dewalt_demand_score = score_dewalt_demand(proj_row, contractor_names)
    inferred_distributors = infer_distributors(contractor_names)
    contractor_rel_score = score_contractor_relationships(contractor_contacts)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Procurement Stage", procurement_stage)
    col2.metric("DEWALT Demand Score", f"{dewalt_demand_score:.2f}")
    col3.metric("Contractor Relationship Score", f"{contractor_rel_score:.2f}")
    col4.metric("Contractors on Project", len(contractor_names))

    if contractor_names:
        st.markdown("**Contractor Ecosystem**")
        st.write(contractor_names)

    if inferred_distributors:
        st.markdown("**Likely Distributor Ecosystem**")
        st.write(inferred_distributors)

def render_account_intelligence(projects_df: pd.DataFrame, merged_all: pd.DataFrame):
    st.subheader("Account Intelligence")

    # Aggregate by canonical_company / developer / owner
    account_dim = "canonical_company" if "canonical_company" in projects_df.columns else None
    if not account_dim:
        st.warning("No canonical_company column found for account intelligence.")
        return

    accounts = projects_df[account_dim].dropna().unique()
    account_selection = st.selectbox("Select Account", accounts)

    acct_projects = projects_df[projects_df[account_dim] == account_selection]
    st.markdown(f"##### Projects for {account_selection}")
    st.dataframe(
        acct_projects[[
            c for c in acct_projects.columns
            if c in ["project_id", "project_name", "developer", "owner",
                     "relationship_status", "relationship_count"]
        ]],
        use_container_width=True
    )

    if "canonical_company" in merged_all.columns:
        acct_contacts = merged_all[
            merged_all["canonical_company"] == account_selection
        ]
    else:
        acct_contacts = merged_all.iloc[0:0].copy()

    st.markdown("##### Contacts at This Account")
    if not acct_contacts.empty:
        cols_to_show = [
            c for c in acct_contacts.columns
            if c in ["full_name", "title", "email", "linkedin_url",
                     "relationship_role", "influence_score"]
        ]
        st.dataframe(acct_contacts[cols_to_show], use_container_width=True)
    else:
        st.info("No contacts mapped to this account yet.")

def render_market_analytics(projects_df: pd.DataFrame, merged_all: pd.DataFrame):
    st.subheader("Market Analytics")

    if "developer" in projects_df.columns:
        dev_counts = projects_df["developer"].value_counts().reset_index()
        dev_counts.columns = ["developer", "projects"]
        fig = px.bar(dev_counts.head(20), x="developer", y="projects",
                     title="Top Developers by Project Count")
        st.plotly_chart(fig, use_container_width=True)

    if "owner" in projects_df.columns:
        owner_counts = projects_df["owner"].value_counts().reset_index()
        owner_counts.columns = ["owner", "projects"]
        fig2 = px.bar(owner_counts.head(20), x="owner", y="projects",
                      title="Top Owners by Project Count")
        st.plotly_chart(fig2, use_container_width=True)

    if "canonical_company" in merged_all.columns:
        contractor_counts = merged_all["canonical_company"].value_counts().reset_index()
        contractor_counts.columns = ["company", "contacts"]
        fig3 = px.bar(contractor_counts.head(20), x="company", y="contacts",
                      title="Top Companies by Relationship Density")
        st.plotly_chart(fig3, use_container_width=True)

def render_exports(projects_df: pd.DataFrame, merged_all: pd.DataFrame):
    st.subheader("Exports")

    st.markdown("##### Export Projects")
    proj_csv = projects_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download Projects CSV",
        data=proj_csv,
        file_name="projects_export.csv",
        mime="text/csv",
    )

    st.markdown("##### Export Project-Relationship View")
    rel_csv = merged_all.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download Relationships CSV",
        data=rel_csv,
        file_name="relationships_export.csv",
        mime="text/csv",
    )

# ====================================================
# MAIN APP
# ====================================================

def main():
    st.set_page_config(
        page_title="Allen Hammett AI — Infrastructure Intelligence OS",
        layout="wide",
    )

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        login_ui()
        return

    st.markdown(
        f"### Allen Hammett AI — Infrastructure Intelligence OS  \n"
        f"**User:** {st.session_state.get('username', 'Executive')}  \n"
        f"**Mode:** Infrastructure Intelligence / DEWALT Demand Sequencing"
    )

    projects_df, merged_all = build_project_relationship_view()

    tabs = st.tabs([
        "Command Wall",
        "Opportunity Operations",
        "Relationship Command",
        "Account Intelligence",
        "Market Analytics",
        "Exports",
    ])

    with tabs[0]:
        render_command_wall(projects_df)

    with tabs[1]:
        render_opportunity_operations(projects_df, merged_all)

    with tabs[2]:
        render_relationship_command(projects_df, merged_all)

    with tabs[3]:
        render_account_intelligence(projects_df, merged_all)

    with tabs[4]:
        render_market_analytics(projects_df, merged_all)

    with tabs[5]:
        render_exports(projects_df, merged_all)


if __name__ == "__main__":
    main()
