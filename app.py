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
