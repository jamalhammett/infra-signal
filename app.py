import os
import pandas as pd
import streamlit as st
import psycopg2
import pydeck as pdk
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Allen Hammett AI", layout="wide")

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


def table_exists(table_name):
    df = run_query(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'public'
          and table_name = %s
        """,
        (table_name,),
    )
    return not df.empty


def get_columns(table_name):
    if not table_exists(table_name):
        return set()

    df = run_query(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
        """,
        (table_name,),
    )
    return set(df["column_name"].tolist())


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


def reset_password(email, reset_key, new_password):
    if reset_key != PASSWORD_RESET_KEY:
        return False, "Invalid reset key."

    if len(new_password) < 10:
        return False, "Password must be at least 10 characters."

    run_execute(
        """
        update users
        set password = %s
        where lower(email) = lower(%s)
        """,
        (new_password, email.strip()),
    )

    return True, "Password updated. You can now log in."


def login_gate():
    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user:
        return st.session_state.user

    st.title("Allen Hammett AI")
    st.subheader("Secure Infrastructure Intelligence Access")

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

    with st.expander("Forgot password / Reset password"):
        reset_email = st.text_input("Account Email", key="reset_email")
        reset_key = st.text_input("Reset Key", type="password", key="reset_key")
        new_password = st.text_input("New Password", type="password", key="new_password")

        if st.button("Reset Password"):
            success, message = reset_password(reset_email, reset_key, new_password)
            if success:
                st.success(message)
            else:
                st.error(message)

    st.stop()


def capture_stage(score):
    if score >= 90:
        return "Prime Positioning"
    if score >= 75:
        return "Strategic Development"
    if score >= 50:
        return "Active Monitoring"
    if score >= 25:
        return "Early Identification"
    return "Historical Context"


def get_projects():
    if not table_exists("projects"):
        return pd.DataFrame()

    df = run_query(
        """
        select *
        from projects
        order by early_capture_score desc nulls last, created_at desc
        limit 5000
        """
    )

    if df.empty:
        return df

    if "early_capture_score" not in df.columns:
        df["early_capture_score"] = 0

    df["early_capture_score"] = pd.to_numeric(
        df["early_capture_score"],
        errors="coerce"
    ).fillna(0).astype(int)

    df["capture_stage"] = df["early_capture_score"].apply(capture_stage)

    return df


def get_leads():
    if not table_exists("leads"):
        return pd.DataFrame()

    cols = get_columns("leads")

    wanted = [
        "company",
        "contact_name",
        "title",
        "email",
        "phone",
        "county",
        "state",
        "source_name",
        "created_at",
    ]

    selected = [c for c in wanted if c in cols]

    if not selected:
        return pd.DataFrame()

    order_clause = "order by created_at desc" if "created_at" in cols else ""

    df = run_query(
        f"""
        select {", ".join(selected)}
        from leads
        {order_clause}
        limit 500
        """
    )

    for c in wanted:
        if c not in df.columns:
            df[c] = None

    return df


user = login_gate()

projects_df = get_projects()
leads_df = get_leads()

st.sidebar.header("Executive Filters")

search = st.sidebar.text_input("Search keyword")
predictive_only = st.sidebar.checkbox("Predictive signals only")
prime_only = st.sidebar.checkbox("Prime positioning only")

filtered_df = projects_df.copy()

if not filtered_df.empty:
    if search:
        filtered_df = filtered_df[
            filtered_df.astype(str).apply(
                lambda row: row.str.contains(search, case=False, na=False).any(),
                axis=1,
            )
        ]

    if predictive_only and "predictive_signal" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["predictive_signal"] == True]

    if prime_only:
        filtered_df = filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]


st.title("Infrastructure Intelligence Platform")
st.markdown("Allen Hammett AI — Executive Infrastructure / Early Capture Intelligence")

st.markdown("### Capture Intelligence Legend")

c1, c2, c3, c4, c5 = st.columns(5)
c1.success("Prime Positioning 90–100")
c2.info("Strategic Development 75–89")
c3.markdown("**Active Monitoring**  \n50–74")
c4.markdown("**Early Identification**  \n25–49")
c5.markdown("**Historical Context**  \n0–24")

m1, m2, m3, m4, m5 = st.columns(5)

m1.metric("Qualified Signals", len(filtered_df))
m2.metric("Prime Positioning", len(filtered_df[filtered_df["capture_stage"] == "Prime Positioning"]) if not filtered_df.empty else 0)
m3.metric("Predictive Signals", len(filtered_df[filtered_df["predictive_signal"] == True]) if "predictive_signal" in filtered_df.columns else 0)
m4.metric("Mapped Records", len(filtered_df.dropna(subset=["latitude", "longitude"])) if {"latitude", "longitude"}.issubset(filtered_df.columns) else 0)
m5.metric("Leads", len(leads_df))

st.markdown("## Infrastructure Intelligence Map")

if {"latitude", "longitude"}.issubset(filtered_df.columns):
    map_df = filtered_df.dropna(subset=["latitude", "longitude"]).copy()

    if not map_df.empty:
        map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
        map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
        map_df = map_df.dropna(subset=["latitude", "longitude"])

        map_df["color"] = map_df["capture_stage"].map({
            "Prime Positioning": [0, 180, 120, 220],
            "Strategic Development": [20, 80, 180, 220],
            "Active Monitoring": [0, 180, 200, 220],
            "Early Identification": [100, 120, 140, 200],
            "Historical Context": [55, 60, 70, 170],
        })

        st.pydeck_chart(
            pdk.Deck(
                map_style=pdk.map_styles.CARTO_DARK,
                initial_view_state=pdk.ViewState(
                    latitude=float(map_df["latitude"].median()),
                    longitude=float(map_df["longitude"].median()),
                    zoom=9,
                    pitch=35,
                ),
                layers=[
                    pdk.Layer(
                        "ScatterplotLayer",
                        data=map_df,
                        get_position="[longitude, latitude]",
                        get_color="color",
                        get_radius=1000,
                        pickable=True,
                        auto_highlight=True,
                    )
                ],
                tooltip={
                    "html": """
                    <b>Project:</b> {canonical_project_name}<br/>
                    <b>Capture Stage:</b> {capture_stage}<br/>
                    <b>Score:</b> {early_capture_score}<br/>
                    <b>Category:</b> {intelligence_category}<br/>
                    <b>County:</b> {county}<br/>
                    <b>Stage:</b> {project_stage}
                    """,
                    "style": {"backgroundColor": "black", "color": "white"},
                },
            )
        )
    else:
        st.warning("No mapped records found.")

st.markdown("## Executive Intelligence Profiles")

if not filtered_df.empty:
    for _, row in filtered_df.head(100).iterrows():
        project_name = row.get("canonical_project_name", "Unnamed Project")
        stage = row.get("capture_stage", "Unknown")

        with st.expander(f"{project_name} ({stage})"):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"**Case Number:** {row.get('case_number', 'N/A')}")
                st.markdown(f"**County:** {row.get('county', 'N/A')}")
                st.markdown(f"**Project Stage:** {row.get('project_stage', 'N/A')}")
                st.markdown(f"**Infrastructure Type:** {row.get('infrastructure_type', 'N/A')}")
                st.markdown(f"**Intelligence Category:** {row.get('intelligence_category', 'N/A')}")
                st.markdown(f"**Capture Score:** {row.get('early_capture_score', 'N/A')}")
                st.markdown(f"**Applicant:** {row.get('applicant_name', 'Unknown')}")
                st.markdown(f"**Utility Provider:** {row.get('utility_dependency', 'Unknown')}")

            with col2:
                st.markdown(f"**Corridor Region:** {row.get('corridor_region', 'N/A')}")
                st.markdown(f"**Market Cluster:** {row.get('market_cluster', 'N/A')}")
                st.markdown(f"**Filing Date:** {row.get('filing_date', 'N/A')}")
                st.markdown(f"**Source Name:** {row.get('source_name', 'N/A')}")
                st.markdown(f"**Source Type:** {row.get('source_type', 'N/A')}")
                st.markdown(f"**Predictive Signal:** {row.get('predictive_signal', False)}")
                st.markdown(f"**Estimated MW Demand:** {row.get('estimated_power_mw', 'Unknown')}")
                st.markdown(f"**Created At:** {row.get('created_at', 'N/A')}")

            st.subheader("Executive Strategic Assessment")
            st.info(row.get("strategic_notes") or "No strategic assessment generated yet.")

            st.subheader("Infrastructure Risk Flags")
            risk_flags = row.get("risk_flags")
            if risk_flags:
                for flag in str(risk_flags).split(","):
                    st.warning(flag.strip())
            else:
                st.success("No major infrastructure risks currently detected.")

            st.subheader("Permit Description")
            st.write(row.get("permit_description") or "No permit description stored yet.")

            st.subheader("Raw Filing Intelligence")
            st.code(str(row.get("raw_text") or "No raw filing text stored yet.")[:5000])

st.markdown("## Executive Priority Intelligence")
st.dataframe(filtered_df, use_container_width=True)

st.markdown("## Infrastructure Leads")
st.dataframe(leads_df, use_container_width=True)

st.markdown("## Export Intelligence")

st.download_button(
    "Download Opportunities CSV",
    filtered_df.to_csv(index=False),
    "infrastructure_opportunities.csv",
    "text/csv",
)

st.download_button(
    "Download Leads CSV",
    leads_df.to_csv(index=False),
    "infrastructure_leads.csv",
    "text/csv",
)

if st.button("Sign Out"):
    st.session_state.user = None
    st.rerun()

st.caption("Allen Hammett AI • Infrastructure Intelligence System")
