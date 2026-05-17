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
    project_type,
    county,
    state
from projects
""")

rows = cur.fetchall()

for row in rows:

    project_id = row[0]

    text = " ".join([
        str(x).lower()
        for x in row
        if x is not None
    ])

    intelligence_category = "General Infrastructure"
    infrastructure_type = "Other"
    strategic_priority = "LOW"
    corridor_region = None
    market_cluster = None

    early_capture_score = 0

    predictive_signal = False
    utility_related = False
    hyperscale_related = False
    transmission_related = False
    fiber_related = False

    # ==========================================
    # HYPERSCALE / DATA CENTER
    # ==========================================

    if any(k in text for k in [
        "data center",
        "datacenter",
        "hyperscale",
        "cloud",
        "server",
        "colo",
        "colocation",
        "digital realty",
        "qts",
        "vantage",
        "cyrusone",
        "stack",
        "equinix"
    ]):

        intelligence_category = "Hyperscale Expansion"
        infrastructure_type = "Data Center"
        strategic_priority = "HIGH"

        hyperscale_related = True
        predictive_signal = True

        early_capture_score += 80

    # ==========================================
    # UTILITY / POWER
    # ==========================================

    if any(k in text for k in [
        "substation",
        "switchyard",
        "dominion",
        "novec",
        "utility",
        "power",
        "energy"
    ]):

        intelligence_category = "Utility Capacity Expansion"
        infrastructure_type = "Utility Infrastructure"

        utility_related = True
        predictive_signal = True

        early_capture_score += 50

    # ==========================================
    # TRANSMISSION
    # ==========================================

    if any(k in text for k in [
        "transmission",
        "grid",
        "powerline",
        "electric"
    ]):

        transmission_related = True

        intelligence_category = "Transmission Infrastructure"

        early_capture_score += 35

    # ==========================================
    # FIBER
    # ==========================================

    if any(k in text for k in [
        "fiber",
        "telecom",
        "broadband"
    ]):

        fiber_related = True

        intelligence_category = "Fiber Corridor Expansion"

        early_capture_score += 25

    # ==========================================
    # INDUSTRIAL
    # ==========================================

    if any(k in text for k in [
        "industrial",
        "warehouse",
        "logistics",
        "manufacturing"
    ]):

        infrastructure_type = "Industrial Infrastructure"

        early_capture_score += 20

    # ==========================================
    # CORRIDOR LOGIC
    # ==========================================

    if any(k in text for k in [
        "ashburn",
        "loudoun"
    ]):

        corridor_region = "Northern Virginia Hyperscale Corridor"

        market_cluster = "Ashburn"

    elif any(k in text for k in [
        "prince william"
    ]):

        corridor_region = "Prince William Expansion Corridor"

        market_cluster = "Prince William"

    elif any(k in text for k in [
        "henrico",
        "richmond"
    ]):

        corridor_region = "Richmond Growth Corridor"

        market_cluster = "Richmond"

    # ==========================================
    # STRATEGIC PRIORITY
    # ==========================================

    if early_capture_score >= 80:
        strategic_priority = "HIGH"

    elif early_capture_score >= 50:
        strategic_priority = "MEDIUM"

    elif early_capture_score >= 25:
        strategic_priority = "WATCHLIST"

    else:
        strategic_priority = "LOW"

    # ==========================================
    # UPDATE
    # ==========================================

    cur.execute("""
    update projects
    set
        intelligence_category = %s,
        infrastructure_type = %s,
        strategic_priority = %s,
        corridor_region = %s,
        market_cluster = %s,
        early_capture_score = %s,
        predictive_signal = %s,
        utility_related = %s,
        hyperscale_related = %s,
        transmission_related = %s,
        fiber_related = %s
    where id = %s
    """, (
        intelligence_category,
        infrastructure_type,
        strategic_priority,
        corridor_region,
        market_cluster,
        early_capture_score,
        predictive_signal,
        utility_related,
        hyperscale_related,
        transmission_related,
        fiber_related,
        project_id
    ))

conn.commit()

cur.close()
conn.close()

print("Project tagging complete.")
