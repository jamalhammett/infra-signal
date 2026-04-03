import os
import re
from pathlib import Path
import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def extract_plan_numbers(html):
    pattern = re.compile(r'\b[A-Z]{3,6}-\d{4}-\d{4}\b')
    return list(set(pattern.findall(html)))

def main():
    raw_dir = Path("raw")
    html_files = sorted(raw_dir.glob("loudoun_land_apps_*.html"), reverse=True)

    if not html_files:
        raise FileNotFoundError("No raw files found")

    latest_file = html_files[0]
    html = latest_file.read_text(encoding="utf-8", errors="ignore")

    plan_numbers = extract_plan_numbers(html)

    print(f"Found {len(plan_numbers)} plan numbers")

    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    # get source id
    cur.execute("""
        select id from source_registry
        where source_name = %s
        limit 1
    """, ("Loudoun Land Applications",))

    source_id = cur.fetchone()[0]

    inserted = 0

    for plan in plan_numbers:
        cur.execute("""
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
        """, (
            source_id,
            "land_application",
            plan,
            plan,
            f"Loudoun project {plan}",
            "Loudoun",
            "VA",
            "Land Application",
            "Detected",
            70
        ))

        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {inserted} signals")

if __name__ == "__main__":
    main()