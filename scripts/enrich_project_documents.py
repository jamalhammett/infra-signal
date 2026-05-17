import os
import requests
import psycopg2 as psycopg
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("""
select
    id,
    canonical_project_name,
    source_url,
    permit_description,
    raw_text
from projects
where source_url is not null
limit 100
""")

rows = cur.fetchall()

for row in rows:

    project_id = row[0]
    project_name = row[1]
    source_url = row[2]
    permit_description = row[3] or ""
    raw_text = row[4] or ""

    combined = f"{project_name} {permit_description} {raw_text}"

    summary = combined[:1500]

    engineering_firm = None
    legal_firm = None
    utility_dependency = None
    estimated_power_mw = None

    text_lower = combined.lower()

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

    if "rezoning" in text_lower:
        risk_flags.append("Zoning Approval Risk")

    if "water" in text_lower:
        risk_flags.append("Water Infrastructure Dependency")

    strategic_notes = f"""
    Executive Infrastructure Assessment:

    Project classified as:
    {project_name}

    Potential infrastructure dependencies detected.

    Recommend:
    - utility relationship mapping
    - contractor ecosystem analysis
    - hyperscale expansion monitoring
    """

    cur.execute("""
    update projects
    set
        document_summary = %s,
        engineering_firm = %s,
        legal_firm = %s,
        utility_dependency = %s,
        risk_flags = %s,
        strategic_notes = %s
    where id = %s
    """, (
        summary,
        engineering_firm,
        legal_firm,
        utility_dependency,
        ", ".join(risk_flags),
        strategic_notes,
        project_id
    ))

conn.commit()
cur.close()
conn.close()

print("Document enrichment complete")
