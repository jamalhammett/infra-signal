import os
import re
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL was not found in .env")


def extract_rows_from_html(html: str):
    """
    Very simple first-pass extractor.
    Looks for plan numbers and nearby fields in the saved HTML.
    This is MVP parsing, not final production parsing.
    """
    results = []

    # Try to find likely plan number patterns like CPAP-2006-0015, STPL-2007-0087, etc.
    plan_number_pattern = re.compile(r'\b[A-Z]{3,6}-\d{4}-\d{4}\b')
    matches = list(plan_number_pattern.finditer(html))

    for m in matches:
        plan_number = m.group(0)

        # Grab a small window around the match so we can infer nearby text
        start = max(0, m.start() - 500)
        end = min(len(html), m.end() + 500)
        snippet = html[start:end]

        # Try to infer plan type
        plan_type = None
        for candidate in [
            "Engineering Plan",
            "Site Plan",
            "Subdivision",
            "Special Exception",
            "Rezoning",
            "Concept Plan",
        ]:
            if candidate.lower() in snippet.lower():
                plan_type = candidate
                break

        # Try to infer plan status
        plan_status = None
        for candidate in [
            "In Review",
            "Approved",
            "Denied",
            "Withdrawn",
            "Pending",
        ]:
            if candidate.lower() in snippet.lower():
                plan_status = candidate
                break

        # Try to infer application date in long form
        date_match = re.search(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}',
            snippet
        )
        hearing_or_app_date = date_match.group(0) if date_match else None

        results.append({
            "plan_number": plan_number,
            "plan_type": plan_type,
            "plan_status": plan_status,
            "application_date_text": hearing_or_app_date,
            "snippet": snippet[:1000]
        })

    # de-duplicate by plan number
    deduped = {}
    for r in results:
        deduped[r["plan_number"]] = r

    return list(deduped.values())


def main():
    raw_dir = Path("raw")
    html_files = sorted(raw_dir.glob("loudoun_land_apps_*.html"), reverse=True)

    if not html_files:
        raise FileNotFoundError("No Loudoun raw HTML files found in raw/")

    latest_file = html_files[0]
    html = latest_file.read_text(encoding="utf-8", errors="ignore")

    parsed_rows = extract_rows_from_html(html)

    print(f"Using file: {latest_file}")
    print(f"Found {len(parsed_rows)} candidate plan rows")

    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    # Find source id for Loudoun
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

    inserted = 0

    for r in parsed_rows:
        case_number = r["plan_number"]
        project_name = r["plan_number"]  # temporary placeholder until we get richer parsing
        signal_type = "land_application"
        project_type = r["plan_type"]
        project_stage = r["plan_status"]
        description = r["snippet"]

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
                signal_type,
                case_number,
                project_name,
                description,
                "Loudoun",
                "VA",
                project_type,
                project_stage,
                40,
            )
        )
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted up to {inserted} signal candidates into signals table.")


if __name__ == "__main__":
    main()