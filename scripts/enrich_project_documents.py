import os
import psycopg2 as psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("""
select
    id,
    canonical_project_name,
    permit_description,
    raw_text,
    infrastructure_type,
    intelligence_category,
    project_stage
from projects
limit 500
""")

rows = cur.fetchall()

for row in rows:

    project_id = row[0]
    project_name = row[1] or ""
    permit_description = row[2] or ""
    raw_text = row[3] or ""
    infrastructure_type = row[4] or ""
    intelligence_category = row[5] or ""
    project_stage = row[6] or ""

    combined = f"""
    {project_name}
    {permit_description}
    {raw_text}
    {infrastructure_type}
    {intelligence_category}
    """

    text_lower = combined.lower()

    engineering_firm = None
    legal_firm = None
    utility_dependency = None
    estimated_power_mw = None

    if "dominion" in text_lower:
        utility_dependency = "Dominion Energy"

    if "novec" in text_lower:
        utility_dependency = "NOVEC"

    if "dewberry" in text_lower:
        engineering_firm = "Dewberry"

    if "kimley-horn" in text_lower:
        engineering_firm = "Kimley-Horn"

    if "mcguirewoods" in text_lower:
        legal_firm = "McGuireWoods"

    if "hunton" in text_lower:
        legal_firm = "Hunton Andrews Kurth"

    risk_flags = []

    if "substation" in text_lower:
        risk_flags.append("Transmission Dependency")

    if "water" in text_lower:
        risk_flags.append("Water Infrastructure Dependency")

    if "rezoning" in text_lower:
        risk_flags.append("Zoning Approval Risk")

    if infrastructure_type == "Data Center":
        estimated_power_mw = 150

    if "hyperscale" in text_lower:
        estimated_power_mw = 300

    strategic_notes = f"""
Executive Infrastructure Assessment

Project:
{project_name}

Classification:
{intelligence_category}

Infrastructure Type:
{infrastructure_type}

Current Stage:
{project_stage}

Strategic Assessment:
This project has potential infrastructure ecosystem implications.

Recommended Actions:
- Monitor utility expansion activity
- Track hyperscale growth indicators
- Evaluate supplier/vendor alignment
- Monitor transmission and fiber dependencies
"""

    document_summary = combined[:2000]

    cur.execute("""
    update projects
    set
        document_summary = %s,
        engineering_firm = %s,
        legal_firm = %s,
        utility_dependency = %s,
        estimated_power_mw = %s,
        risk_flags = %s,
        strategic_notes = %s
    where id = %s
    """, (
        document_summary,
        engineering_firm,
        legal_firm,
        utility_dependency,
        estimated_power_mw,
        ", ".join(risk_flags),
        strategic_notes,
        project_id
    ))

conn.commit()
cur.close()
conn.close()

print("AI enrichment complete")
