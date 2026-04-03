import os
import re
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")


def main():
    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        select id, storage_path
        from raw_documents
        order by fetched_at desc
        limit 2
    """)
    rows = cur.fetchall()

    if len(rows) < 2:
        print("Not enough snapshots to compare.")
        cur.close()
        conn.close()
        return

    latest_id, latest_path = rows[0]
    previous_id, previous_path = rows[1]

    latest_text = Path(latest_path).read_text(encoding="utf-8", errors="ignore")
    previous_text = Path(previous_path).read_text(encoding="utf-8", errors="ignore")

    patterns = [
        r"ZMAP-\d{4}-\d+",
        r"SPEX-\d{4}-\d+",
        r"CPAP-\d{4}-\d+",
        r"STPL-\d{4}-\d+",
        r"ZCPA-\d{4}-\d+",
        r"SPMI-\d{4}-\d+",
    ]

    latest_hits = set()
    previous_hits = set()

    for pattern in patterns:
        latest_hits.update(re.findall(pattern, latest_text))
        previous_hits.update(re.findall(pattern, previous_text))

    new_hits = latest_hits - previous_hits

    if not new_hits:
        print("No new case-number style signals detected.")
        cur.close()
        conn.close()
        return

    print(f"Found {len(new_hits)} new signal(s):")

    source_name = "Loudoun Land Applications"

    cur.execute("""
        select id
        from source_registry
        where source_name = %s
        limit 1
    """, (source_name,))
    source_row = cur.fetchone()

    if not source_row:
        raise ValueError("Could not find Loudoun source in source_registry")

    source_id = source_row[0]

    for hit in sorted(new_hits):
        print("New signal detected:", hit)

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
            values (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                now()
            )
            on conflict do nothing
        """, (
            source_id,
            "land_application",
            hit,
            hit,
            f"Detected from snapshot comparison between raw_documents {previous_id} and {latest_id}",
            "Loudoun",
            "VA",
            "Land Application",
            "Detected",
            70,
        ))

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()