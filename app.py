# =========================================================
# PHASE 11B — TRADE PACKAGE + DEWALT DEMAND ENGINE
# =========================================================

CONTRACTOR_ECOSYSTEM = {
    "Turner": {
        "category": "GC",
        "dewalt_alignment": "Very High"
    },
    "Holder": {
        "category": "GC",
        "dewalt_alignment": "High"
    },
    "Whiting-Turner": {
        "category": "GC",
        "dewalt_alignment": "Very High"
    },
    "HITT": {
        "category": "GC",
        "dewalt_alignment": "High"
    },
    "DPR": {
        "category": "GC",
        "dewalt_alignment": "Very High"
    },
    "AECOM": {
        "category": "Engineering",
        "dewalt_alignment": "Medium"
    },
    "Rosendin": {
        "category": "Electrical",
        "dewalt_alignment": "Very High"
    },
    "M.C. Dean": {
        "category": "Electrical",
        "dewalt_alignment": "Very High"
    }
}


def infer_project_phase(project_name):

    name = str(project_name).lower()

    if any(x in name for x in [
        "rezoning",
        "land use",
        "site plan",
        "permit",
        "application",
        "floodplain"
    ]):
        return "Pre-Construction"

    if any(x in name for x in [
        "substation",
        "utility",
        "transmission",
        "power"
    ]):
        return "Power + Utility Infrastructure"

    if any(x in name for x in [
        "grading",
        "civil",
        "sitework",
        "earthwork"
    ]):
        return "Site Development"

    if any(x in name for x in [
        "steel",
        "shell",
        "structure"
    ]):
        return "Structural Buildout"

    if any(x in name for x in [
        "fit out",
        "interior",
        "commissioning"
    ]):
        return "Commissioning"

    return "Core Construction"


def next_buying_window(phase):

    mapping = {
        "Pre-Construction":
            "Engineering + specification positioning",

        "Power + Utility Infrastructure":
            "Electrical contractor engagement",

        "Site Development":
            "Civil + concrete procurement",

        "Structural Buildout":
            "Steel + fastening + anchor systems",

        "Core Construction":
            "High-volume cordless + trade deployment",

        "Commissioning":
            "Operations + maintenance conversion"
    }

    return mapping.get(phase, "General contractor engagement")


def dewalt_product_push(phase):

    products = {
        "Pre-Construction":
            "Laser measurement, layout tools, BIM support",

        "Power + Utility Infrastructure":
            "Electrical tools, bandsaws, knockout systems, storage",

        "Site Development":
            "Concrete anchors, outdoor power equipment, generators",

        "Structural Buildout":
            "Fastening systems, drills, impacts, anchors",

        "Core Construction":
            "20V MAX platform, FLEXVOLT, storage, safety gear",

        "Commissioning":
            "Maintenance kits, diagnostics, service contracts"
    }

    return products.get(phase, "General DEWALT deployment")


def sales_action(score, relationships):

    if score >= 120 and relationships >= 25:
        return "EXECUTIVE PURSUIT"

    if score >= 90:
        return "CONTRACTOR ENGAGEMENT"

    if relationships >= 10:
        return "EXPAND ACCOUNT PENETRATION"

    return "BUILD RELATIONSHIP COVERAGE"


# =========================================================
# BUILD ENRICHED OPPORTUNITY DATAFRAME
# =========================================================

def build_dewalt_intelligence_layer(df):

    enriched_rows = []

    for _, row in df.iterrows():

        phase = infer_project_phase(
            row.get("canonical_project_name", "")
        )

        relationships = int(
            row.get("relationship_inventory", 0)
        )

        score = int(
            row.get("max_capture_score", 0)
        )

        enriched_rows.append({
            "project":
                row.get("canonical_project_name", ""),

            "account":
                row.get("account_name", ""),

            "market":
                row.get("market", "Ashburn"),

            "phase":
                phase,

            "buying_window":
                next_buying_window(phase),

            "dewalt_strategy":
                dewalt_product_push(phase),

            "sales_action":
                sales_action(score, relationships),

            "relationships":
                relationships,

            "capture_score":
                score,

            "mw":
                row.get("total_mw", 0)
        })

    return pd.DataFrame(enriched_rows)


# =========================================================
# DEWALT COMMAND CENTER TAB
# =========================================================

with tab_market:

    st.markdown("## DEWALT Strategic Demand Engine")

    dewalt_df = build_dewalt_intelligence_layer(account_df)

    st.dataframe(
        dewalt_df,
        use_container_width=True,
        height=700
    )

    st.markdown("---")

    st.markdown("## Executive Pursuit Targets")

    pursuit_df = dewalt_df[
        dewalt_df["sales_action"] == "EXECUTIVE PURSUIT"
    ]

    st.dataframe(
        pursuit_df,
        use_container_width=True,
        height=400
    )

    st.markdown("---")

    st.markdown("## Phase Distribution")

    phase_counts = (
        dewalt_df["phase"]
        .value_counts()
        .reset_index()
    )

    phase_counts.columns = ["Phase", "Projects"]

    st.bar_chart(
        phase_counts.set_index("Phase")
    )
