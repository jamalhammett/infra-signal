import os
import time
import requests
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
    county,
    state
from projects
where latitude is null
   or longitude is null
limit 500
""")

rows = cur.fetchall()

HEADERS = {
    "User-Agent": "AllenHammettAI/1.0"
}

def geocode_location(query):

    url = "https://nominatim.openstreetmap.org/search"

    params = {
        "q": query,
        "format": "json",
        "limit": 1
    }

    try:
        response = requests.get(
            url,
            params=params,
            headers=HEADERS,
            timeout=20
        )

        data = response.json()

        if not data:
            return None, None

        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])

        return lat, lon

    except Exception as e:
        print(f"Geocode error: {e}")
        return None, None


updated = 0

for row in rows:

    project_id = row[0]
    project_name = row[1]
    county = row[2]
    state = row[3]

    query = f"{project_name}, {county}, {state}"

    print(f"Geocoding: {query}")

    lat, lon = geocode_location(query)

    if lat and lon:

        cur.execute("""
        update projects
        set
            latitude = %s,
            longitude = %s
        where id = %s
        """, (
            lat,
            lon,
            project_id
        ))

        updated += 1

    time.sleep(1)

conn.commit()

cur.close()
conn.close()

print(f"Geocoding complete. Updated: {updated}")
