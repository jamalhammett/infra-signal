import os
import re
import subprocess

import pandas as pd
import psycopg2 as psycopg
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))
SUPABASE_URL = st.secrets.get("SUPABASE_URL", os.getenv("SUPABASE_URL"))
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY", os.getenv("SUPABASE_ANON_KEY"))

st.set_page_config(
    page_title="Infrastructure Intelligence Platform",
    layout="wide"
)


# =========================
# DATABASE
# =========================
def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = psycopg.connect(DATABASE_URL)
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


# =========================
# SUPABASE AUTH
# =========================
def authenticate_user(email: str, password: str):
    url = f"{SUPABASE_URL}/auth/v1/token?grant_type=password"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"email": email, "password": password}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=20)
    except Exception:
        return None

    if response.status_code != 200:
        return None

    return response.json()


def send_password_recovery(email: str):
    url = f"{SUPABASE_URL}/auth/v1/recover"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "email": email,
        "redirect_to": "https://infra-signal.streamlit.app/"
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=20)
        return response.status_code in [200, 204]
    except Exception:
        return False


def update_password_with_token(access_token: str, new_password: str):
    url = f"{SUPABASE_URL}/auth/v1/user"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    payload = {"password": new_password}

    try:
        response = requests.put(url, headers=headers, json=payload, timeout=20)
        return response.status_code == 200
    except Exception:
        return False


def get_user_profile(email: str):
    df = run_query(
        """
        select email, full_name, company, role, access_status, access_expires_at
        from user_profiles
        where lower(email) = lower(%s)
        limit 1
        """,
        (email,),
    )

    if df.empty:
        return None

    return df.iloc[0].to_dict()


def handle_password_recovery():
    params = st.query_params

    access_token = params.get("access_token", None)
    recovery_type = params.get("type", None)

    if isinstance(access_token, list):
        access_token = access_token[0]

    if isinstance(recovery_type, list):
        recovery_type = recovery_type[0]

    if recovery_type == "recovery" and access_token:
        st.title("Reset Your Password")
        st.caption("Allen Hammett AI — Secure Access Recovery")

        new_password = st.text_input("New Password", type="password")
        confirm_password = st.text_input("Confirm New Password", type="password")

        if st.button("Update Password"):
            if not new_password or not confirm_password:
                st.error("Enter and confirm your new password.")
                st.stop()

            if new_password != confirm_password:
                st.error("Passwords do not match.")
                st.stop()

            if len(new_password) < 10:
                st.error("Password must be at least 10 characters.")
                st.stop()

            updated = update_password_with_token(access_token, new_password)

            if updated:
                st.success("Password updated. You can now sign in.")
                st.query_params.clear()
                st.stop()
            else:
                st.error("Password update failed. Request a new reset link.")
                st.stop()

        st.stop()


def login_screen():
    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user:
        return st.session_state.user

    st.title("Allen Hammett AI")
    st.subheader("Private Infrastructure Intelligence Access")

    tab1, tab2 = st.tabs(["Sign In", "Forgot Password"])

    with tab1:
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_password")

        if st.button("Sign In"):
            if not email or not password:
                st.error("Enter email and password.")
                st.stop()

            auth_result = authenticate_user(email.strip().lower(), password)

            if not auth_result:
                st.error("Invalid login.")
                st.stop()

            profile = get_user_profile(email.strip().lower())

            if not profile:
                st.error("Your account exists, but access has not been approved.")
                st.stop()

            if profile["access_status"] != "active":
                st.error("Your access is not active.")
                st.stop()

            expires_at = profile.get("access_expires_at")

            if expires_at is not None:
                expires_at = pd.to_datetime(expires_at, utc=True)
                now_utc = pd.Timestamp.now(tz="UTC")

                if expires_at < now_utc:
                    st.error("Your preview access has expired. Contact Allen Hammett Inc to continue access.")
                    st.stop()

            st.session_state.user = profile
            st.rerun()

    with tab2:
        recovery_email = st.text_input("Account Email", key="recovery_email")

        if st.button("Send Password Reset Link"):
            if not recovery_email:
                st.error("Enter your email.")
                st.stop()

            sent = send_password_recovery(recovery_email.strip().lower())

            if sent:
                st.success("Password reset email sent. Check your inbox.")
            else:
                st.error("Could not send reset email.")

    st.stop()


# =========================
# ADMIN REFRESH CONTROLS
# =========================
def admin_refresh_controls(role):
    if role != "admin":
        return

    st.sidebar.header("Admin Controls")

    if st.sidebar.button("Refresh Infrastructure Signals"):
        st.sidebar.info("Refresh started...")

        env = os.environ.copy()
        env["DATABASE_URL"] = DATABASE_URL

        try:
            result1 = subprocess.run(S
                [sys.executable, "scripts/generate_signals_from_api.py"],
                check=True,
                timeout=180,
                capture_output=True,
                text=True,
                env=env
            )

            result2 = subprocess.run(
                ["python", "scripts/promote_signals_to_projects.py"],
                check=True,
                timeout=180,
                capture_output=True,
                text=True,
                env=env
            )

            st.sidebar.success("Signals refreshed. Refresh the page.")
            st.sidebar.code(result1.stdout + "\n" + result2.stdout)

        except subprocess.CalledProcessError as e:
            st.sidebar.error("Refresh failed.")
            st.sidebar.code((e.stdout or "") + "\n" + (e.stderr or ""))

        except Exception as e:
            st.sidebar.error("Refresh failed.")
            st.sidebar.code(str(e))


# =========================
# SAFETY CHECKS
# =========================
if not DATABASE_URL:
    st.error("DATABASE_URL not found.")
    st.stop()

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("SUPABASE_URL or SUPABASE_ANON_KEY not found in Streamlit secrets.")
    st.stop()


handle_password_recovery()

user = login_screen()
role = user["role"]

admin_refresh_controls(role)

st.title("Infrastructure Intelligence Platform")
st.caption(f"Allen Hammett AI — Private Access Preview | {user.get('company', '')}")
st.success("🟢 Action-only mode: showing prioritized infrastructure targets with BD guidance.")


# =========================
# HELPERS
# =========================
def clean_text(value) -> str:
    if value is None or pd.isnull(value):
        return ""
    return str(value).strip()


def extract_filing_year(case_number: str) -> str:
    text = clean_text(case_number)
    match = re.search(r"(20\d{2}|19\d{2})", text)
    return match.group(1) if match else "Unknown"


def extract_target_company(project_name: str, description: str) -> str:
    name = clean_text(project_name)
    desc = clean_text(description)
    combined = f"{name} {desc}"

    known_companies = [
        "CyrusOne",
        "Vantage Data Centers",
        "Vantage",
        "QTS",
        "Digital Realty",
        "STACK",
        "Switch",
        "CoreSite",
        "Aligned",
        "NOVEC",
        "Dominion Energy",
        "BlackChamber Group",
        "Intergate",
        "LC3",
        "Amazon",
        "Meta",
        "Google",
        "Microsoft",
        "Compass",
        "EdgeCore",
        "Equinix",
        "Iron Mountain",
        "Cologix",
    ]

    for company in known_companies:
        if company.lower() in combined.lower():
            return company

    patterns = [
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\s(?:LLC|Inc|Corp|Corporation|LP|LLP|Ltd))\b",
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\sData Center(?:s)?)\b",
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\sGroup)\b",
        r"\b([A-Z][A-Za-z0-9&\-\.\s]+?\sEnergy)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, combined)
        if match:
            return match.group(1).strip()

    low_value_names = {"", "none", "unknown", "gis update"}

    if name and name.lower() not in low_value_names and len(name) >= 4:
        return name

    return "Unknown Target"


def infer_target_role(project_type, project_stage, description, project_name):
    ptype = clean_text(project_type).lower()
    stage = clean_text(project_stage).lower()
    desc = clean_text(description).lower()
    name = clean_text(project_name).lower()
    combined = f"{ptype} {stage} {desc} {name}"

    if "substation" in combined or "dominion" in combined or "novec" in combined:
        return "Utility / Power Infrastructure Lead"

    if "performance bond" in combined:
        return "General Contractor / Construction Manager"

    if stage in ["approved", "in review", "pending", "submitted", "submitted - online"]:
        if "data center" in combined or "datacenter" in combined:
            return "Developer / Project Delivery Lead"
        if "warehouse" in combined or "industrial" in combined or "commercial" in combined:
            return "Developer / Construction Lead"

    if "planning correspondence" in combined:
        return "Developer / Design Team"

    if "legislative" in combined or "rezoning" in combined:
        return "Developer / Real Estate Lead"

    return "Developer / Owner Representative"


def timing_bucket(date_value):
    if pd.isnull(date_value):
        return "Unknown"

    try:
        ts = pd.Timestamp(date_value)

        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        days = (pd.Timestamp.now(tz="UTC") - ts).days

        if days <= 90:
            return "🟢 Immediate"
        if days <= 180:
            return "🟡 Near-Term"
        return "🔵 Long-Term"
    except Exception:
        return "Unknown"


def days_since(date_value):
    if pd.isnull(date_value):
        return 9999

    try:
        ts = pd.Timestamp(date_value)

        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        return int((pd.Timestamp.now(tz="UTC") - ts).days)
    except Exception:
        return 9999


def relevance_score(project_name, project_type, description):
    combined = f"{clean_text(project_name)} {clean_text(project_type)} {clean_text(description)}".lower()
    score = 0

    for kw in [
        "data center",
        "datacenter",
        "substation",
        "warehouse",
        "industrial",
        "commercial",
        "cloud",
        "server",
        "switchyard",
        "power",
        "utility",
    ]:
        if kw in combined:
            score += 20

    if any(x in combined for x in [
        "cyrus", "vantage", "qts", "digital realty", "stack",
        "coresite", "aligned", "intergate", "novec", "dominion",
        "blackchamber", "cologix", "edgecore", "equinix"
    ]):
        score += 25

    if any(x in combined for x in ["agricultural", "forestal", "vineyard", "gis update"]):
        score -= 100

    return score


def stage_score(project_stage, project_type, description):
    stage = clean_text(project_stage).lower()
    combined = f"{stage} {clean_text(project_type)} {clean_text(description)}".lower()
    score = 0

    if stage == "approved":
        score += 40
    elif stage == "in review":
        score += 30
    elif stage in ["pending", "submitted", "submitted - online"]:
        score += 15

    if "performance bond" in combined:
        score += 35

    if "amendment" in combined:
        score += 20

    return score


def freshness_score(date_value):
    d = days_since(date_value)

    if d <= 90:
        return 30
    if d <= 180:
        return 20
    if d <= 365:
        return 10
    return 0


def targetability_score(target_company, target_role, description):
    score = 0

    if clean_text(target_company).lower() != "unknown target":
        score += 20

    if clean_text(target_role):
        score += 20

    if clean_text(description):
        score += 10

    return score


def why_this_matters(row):
    combined = f"{row['project_type']} {row['project_stage']} {row['project_name']} {row['project_description']}".lower()

    if "substation" in combined or "dominion" in combined or "novec" in combined:
        return "Power infrastructure movement often signals major site-readiness and downstream construction spend."

    if "amendment" in combined and ("data center" in combined or "datacenter" in combined):
        return "A data center amendment often indicates expansion activity and a new vendor influence window."

    if "approved" in combined and ("data center" in combined or "datacenter" in combined):
        return "Approved data center work suggests the project is moving toward procurement and execution."

    if "in review" in combined and ("data center" in combined or "datacenter" in combined):
        return "An in-review data center project creates a window to influence stakeholders before award paths harden."

    if "performance bond" in combined:
        return "Performance bond activity is a late pre-construction signal and often precedes field mobilization."

    return "This signal shows active infrastructure movement with potential downstream procurement value."


def recommended_move(row):
    role_value = clean_text(row["Target Role"]).lower()

    if "utility" in role_value:
        return "Approach power and site-infrastructure stakeholders around site readiness, contractor activity, and field support needs."

    if "general contractor" in role_value or "construction manager" in role_value:
        return "Reach the GC / CM now to position DeWalt as the jobsite standard before mobilization accelerates."

    if "developer / project delivery lead" in role_value:
        return "Approach developer-side project delivery leadership now to map procurement timing and identify execution partners."

    if "developer / design team" in role_value:
        return "Begin early relationship-building with the development/design team before final execution partners are locked."

    if "developer / real estate lead" in role_value:
        return "Treat this as strategic account mapping and identify when the project moves from land control into execution."

    return "Use this signal to identify the delivery-side decision path before competitors establish the relationship."


# =========================
# DATA LOAD
# =========================
df = run_query("""
select
    case_number,
    project_name,
    project_type,
    project_stage,
    created_at,
    project_description
from signals
where (
        created_at >= now() - interval '365 days'
        or project_stage in ('Approved', 'In Review', 'Pending', 'Submitted', 'Submitted - Online')
      )
order by created_at desc
limit 1500
""")

df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
df["Filing Year"] = df["case_number"].apply(extract_filing_year)
df["Target Company"] = df.apply(lambda row: extract_target_company(row["project_name"], row["project_description"]), axis=1)
df["Target Role"] = df.apply(lambda row: infer_target_role(row["project_type"], row["project_stage"], row["project_description"], row["project_name"]), axis=1)
df["Timing"] = df["created_at"].apply(timing_bucket)
df["Days Since Signal"] = df["created_at"].apply(days_since)

df["Actionability Score"] = df.apply(
    lambda row: (
        relevance_score(row["project_name"], row["project_type"], row["project_description"])
        + stage_score(row["project_stage"], row["project_type"], row["project_description"])
        + freshness_score(row["created_at"])
        + targetability_score(row["Target Company"], row["Target Role"], row["project_description"])
    ),
    axis=1
)

df["Opportunity Stage"] = df["Actionability Score"].apply(
    lambda score: "🟢 Act Now" if score >= 85 else "🟡 Position Early" if score >= 60 else "🔵 Research Queue"
)

df["Why This Matters"] = df.apply(why_this_matters, axis=1)
df["Recommended Move"] = df.apply(recommended_move, axis=1)

junk_mask = (
    df["project_name"].fillna("").str.lower().str.contains("agricultural|forestal|vineyard|gis update", regex=True)
    | df["project_type"].fillna("").str.lower().str.contains("agricultural|forestal|gis update", regex=True)
    | df["project_description"].fillna("").str.lower().str.contains("agricultural|forestal|vineyard|gis update", regex=True)
    | df["project_name"].isna()
)

df["Is Junk"] = junk_mask

action_df = df[
    (df["Is Junk"] == False)
    & (df["Actionability Score"] >= 60)
    & (df["Target Company"] != "Unknown Target")
].copy()

research_df = df[
    (df["Is Junk"] == False)
    & (
        (df["Actionability Score"] < 60)
        | (df["Target Company"] == "Unknown Target")
    )
].copy()

action_df = action_df.sort_values(by=["Actionability Score", "created_at"], ascending=[False, False])
research_df = research_df.sort_values(by=["Actionability Score", "created_at"], ascending=[False, False])

act_now_df = action_df[action_df["Opportunity Stage"] == "🟢 Act Now"].copy()
position_df = action_df[action_df["Opportunity Stage"] == "🟡 Position Early"].copy()

if role == "trial_viewer":
    act_now_df = act_now_df.head(5)
    position_df = position_df.head(5)
    research_df = research_df.head(0)

elif role == "bd_viewer":
    act_now_df = act_now_df.head(25)
    position_df = position_df.head(25)
    research_df = research_df.head(0)

elif role == "executive_full":
    act_now_df = act_now_df.head(100)
    position_df = position_df.head(100)

elif role == "admin":
    pass


# =========================
# DASHBOARD
# =========================
col1, col2, col3, col4 = st.columns(4)
col1.metric("Act Now Targets", len(act_now_df))
col2.metric("Position Early Targets", len(position_df))
col3.metric("Research Queue", len(research_df))
latest_ts = df["created_at"].max()
col4.metric("Latest Signal Date", "Unknown" if pd.isnull(latest_ts) else str(latest_ts.date()))

st.header("🟢 Immediate Opportunity Signals")

if act_now_df.empty:
    st.info("No immediate targets found.")
else:
    for _, row in act_now_df.head(8).iterrows():
        st.success(
            f"{row['Target Company']} — {row['project_name']} | {row['project_stage']} | Target: {row['Target Role']}"
        )

st.header("🟢 Act Now")

st.dataframe(
    act_now_df[
        [
            "case_number",
            "Filing Year",
            "Target Company",
            "project_name",
            "project_type",
            "project_stage",
            "Timing",
            "Target Role",
            "Actionability Score",
            "Recommended Move",
        ]
    ],
    use_container_width=True,
)

st.header("🟡 Position Early")

st.dataframe(
    position_df[
        [
            "case_number",
            "Filing Year",
            "Target Company",
            "project_name",
            "project_type",
            "project_stage",
            "Timing",
            "Target Role",
            "Actionability Score",
            "Recommended Move",
        ]
    ],
    use_container_width=True,
)

if role in ["admin", "executive_full"]:
    with st.expander("🔵 Research Queue", expanded=False):
        st.dataframe(
            research_df[
                [
                    "case_number",
                    "Filing Year",
                    "Target Company",
                    "project_name",
                    "project_type",
                    "project_stage",
                    "Timing",
                    "Target Role",
                    "Actionability Score",
                ]
            ].head(100),
            use_container_width=True,
        )

st.header("Project Detail")

detail_df = action_df.copy()
detail_df["Detail Key"] = detail_df.apply(
    lambda row: f"{clean_text(row['case_number'])} | {clean_text(row['project_name'])}",
    axis=1,
)

if role == "trial_viewer":
    detail_df = detail_df.head(5)

if detail_df.empty:
    st.info("No projects available for detail view.")
else:
    selected_key = st.selectbox("Select a project", detail_df["Detail Key"].tolist())
    detail = detail_df[detail_df["Detail Key"] == selected_key].iloc[0]

    st.subheader(str(detail.get("project_name", "Unknown Project")))

    left, right = st.columns(2)

    with left:
        st.write(f"**Case Number:** {detail.get('case_number', '')}")
        st.write(f"**Filing Year:** {detail.get('Filing Year', 'Unknown')}")
        st.write(f"**Project Type:** {detail.get('project_type', 'Unknown')}")
        st.write(f"**Project Stage:** {detail.get('project_stage', 'Unknown')}")
        st.write(f"**Timing:** {detail.get('Timing', 'Unknown')}")
        st.write(f"**Opportunity Stage:** {detail.get('Opportunity Stage', 'Unknown')}")
        st.write(f"**Actionability Score:** {detail.get('Actionability Score', 'Unknown')}")

    with right:
        st.write(f"**Target Company:** {detail.get('Target Company', 'Unknown')}")
        st.write(f"**Target Role:** {detail.get('Target Role', 'Unknown')}")
        st.write(f"**Days Since Signal:** {detail.get('Days Since Signal', 'Unknown')}")

    st.markdown("### Why This Matters")
    st.write(detail.get("Why This Matters", ""))

    st.markdown("### Recommended Move")
    st.write(detail.get("Recommended Move", ""))

    st.markdown("### Description")
    st.write(detail.get("project_description", ""))

if role == "admin":
    with st.expander("Admin: User Access Overview", expanded=False):
        users_df = run_query("""
        select email, full_name, company, role, access_status, access_expires_at, created_at
        from user_profiles
        order by created_at desc
        """)
        st.dataframe(users_df, use_container_width=True)

if st.button("Sign Out"):
    st.session_state.user = None
    st.rerun()

st.info("Allen Hammett AI — Infrastructure Intelligence | Confidential Preview")
