import os

import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")

URL = "https://logis.loudoun.gov/gis/rest/services/Projects/LOLA_DATA/MapServer/0/query?where=1=1&outFields=*&returnGeometry=false&f=json"

# First-pass test size so you can confirm inserts quickly.
# Change to None later when ready for all records.
TEST_LIMIT = None


def main():
    try:
        response = requests.get(URL, timeout=60)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        print(f"Found {len(features)} records", flush=True)

        if TEST_LIMIT is not None:
            features = features[:TEST_LIMIT]
            print(f"Testing first {len(features)} records", flush=True)

        conn = psycopg.connect(DATABASE_URL)
        cur = conn.cursor()

        cur.execute(
            """
            select id
            from source_registry
            where source_name = %s
            limit 1
            """,
            ("Loudoun Land Applications",)
        )

        row = cur.fetchone()
        if not row:
            raise ValueError("Could not find source_registry row for Loudoun Land Applications")

        source_id = row[0]
        print(f"Using source_id: {source_id}", flush=True)

        inserted = 0
        skipped = 0

        for feature in features:
            attrs = feature.get("attributes", {})

            case_number = attrs.get("PlanNumber")
            project_name = attrs.get("PlanName")
            project_type = attrs.get("PlanType")
            project_stage = attrs.get("PlanStatus")
            description = attrs.get("PlanDescription")

            if not case_number:
                skipped += 1
                continue

            cur.execute(
                """
                insert into signals (
                    source_id,
                    signal_type,
                    case_number,
                    project_name,
                    project_description,
                    county,
                    state,
                    project_type,
                    project_stage,
                    confidence_score,
                    created_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict do nothing
                """,
                (
                    source_id,
                    "land_application",
                    case_number,
                    project_name,
                    description,
                    "Loudoun",
                    "VA",
                    project_type,
                    project_stage,
                    90,
                )
            )

            inserted += 1

            if inserted % 500 == 0:
                print(f"Inserted {inserted} so far...", flush=True)

        conn.commit()
        cur.close()
        conn.close()

        print(f"Inserted up to {inserted} signals", flush=True)
        print(f"Skipped {skipped} records with no case_number", flush=True)

    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", flush=True)
        raise


if __name__ == "__main__":
    main()