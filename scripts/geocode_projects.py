import os
import time
import requests
import psycopg2 as psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")

TARGET_KEYWORDS = [
    "data center",
    "datacenter",
    "substation",
    "transmission",
    "switchyard",
    "utility",
    "power",
    "energy",
    "fiber",
    "telecom",
    "hyperscale",
    "industrial",
    "cyrusone",
    "vantage",
    "qts",
    "digital realty",
    "stack",
    "equinix",
    "aligned",
    "cologix",
    "novec",
    "dominion",
]

EXCLUDED_KEYWORDS = [
    "sidewalk",
    "trail",
    "church",
    "school",
    "vineyard",
    "farm",
    "subdivision",
    "townhome",
    "townhomes",
    "single family",
    "residential",
    "lot ",
    "lots",
    "playground",
    "landscape",
    "forest",
    "stormwater",
]

HEADERS = {
    "User-Agent": "AllenHammettAI-InfrastructureIntelligence/1.0"
}


def is_target(text):
    text = (text or "").lower()

    if any(bad in text for bad in EXCLUDED_KEYWORDS):
        return False

    return any(good in text for good in TARGET_KEYWORDS)


def geocode_location(query):
    url = "https://nominatim.openstreetmap.org/search"

    params = {
        "q": query,
        "format": "json",
        "limit": 1,
        "countrycodes": "us",
    }

    try:
        response = requests.get(
            url,
            params=params,
            headers=HEADERS,
            timeout=20,
        )

        if response.status_code != 200:
            print(f"Geocode failed status {response.status_code}: {query}", flush=True)
            return None, None

        data = response.json()

        if not data:
            return None, None

        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])

        return lat, lon

    except Exception as e:
        print(f"Geocode error for {query}: {e}", flush=True)
        return None, None


def main():
    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute(
        """
        select
            id,
            canonical_project_name,
            project_type,
            county,
            state,
            intelligence_category,
            infrastructure_type,
            early_capture_score
        from projects
        where latitude is null
           or longitude is null
        order by early_capture_score desc nulls last, created_at desc
        limit 300
        """
    )

    rows = cur.fetchall()

    reviewed = 0
    skipped = 0
    updated = 0

    for row in rows:
        reviewed += 1

        project_id = row[0]
        project_name = row[1]
        project_type = row[2]
        county = row[3] or "Loudoun"
        state = row[4] or "VA"
        intelligence_category = row[5]
        infrastructure_type = row[6]
        score = row[7] or 0

        combined_text = " ".join(
            [
                str(project_name or ""),
                str(project_type or ""),
                str(intelligence_category or ""),
                str(infrastructure_type or ""),
                str(county or ""),
                str(state or ""),
            ]
        )

        if not is_target(combined_text):
            skipped += 1
            continue

        if not project_name:
            skipped += 1
            continue

        query = f"{project_name}, {county} County, {state}, USA"

        print(f"Geocoding target: {query}", flush=True)

        lat, lon = geocode_location(query)

        if lat is not None and lon is not None:
            cur.execute(
                """
                update projects
                set
                    latitude = %s,
                    longitude = %s
                where id = %s
                """,
                (lat, lon, project_id),
            )

            updated += 1

        time.sleep(1)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Projects reviewed: {reviewed}", flush=True)
    print(f"Skipped non-targets: {skipped}", flush=True)
    print(f"Geocoding complete. Updated: {updated}", flush=True)


if __name__ == "__main__":
    main()
