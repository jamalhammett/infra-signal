import os
from datetime import datetime, UTC
from pathlib import Path

import httpx
import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL was not found in .env")

RAW_DIR = Path("raw")
RAW_DIR.mkdir(exist_ok=True)

SOURCE_NAME = "Loudoun Land Applications"


def main():
    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute(
        """
        select id, county, state, source_name, source_url
        from source_registry
        where source_name = %s
        and is_active = true
        limit 1
        """,
        (SOURCE_NAME,),
    )
    source = cur.fetchone()

    if not source:
        raise ValueError(f"Source not found: {SOURCE_NAME}")

    source_id, county, state, source_name, source_url = source

    print(f"Found source: {source_name}")
    print(f"Fetching: {source_url}")

    cur.execute(
        """
        insert into source_runs (source_id, status, run_started_at)
        values (%s, %s, now())
        returning id
        """,
        (source_id, "running"),
    )
    source_run_id = cur.fetchone()[0]
    conn.commit()

    try:
        response = httpx.get(source_url, timeout=30.0, follow_redirects=True)
        response.raise_for_status()

        content = response.text
        fetched_at = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        filename = f"loudoun_land_apps_{fetched_at}.html"
        file_path = RAW_DIR / filename

        file_path.write_text(content, encoding="utf-8")

        cur.execute(
            """
            insert into raw_documents (
                source_id,
                source_run_id,
                document_url,
                storage_path,
                content_type,
                fetched_at,
                raw_text
            )
            values (%s, %s, %s, %s, %s, now(), %s)
            returning id
            """,
            (
                source_id,
                source_run_id,
                source_url,
                str(file_path),
                response.headers.get("content-type", "text/html"),
                content,
            ),
        )
        raw_document_id = cur.fetchone()[0]

        cur.execute(
            """
            insert into raw_snapshot_reviews (
                raw_document_id,
                review_status
            )
            values (%s, %s)
            """,
            (raw_document_id, "pending"),
        )

        cur.execute(
            """
            update source_runs
            set run_finished_at = now(),
                status = %s,
                records_found = %s,
                records_inserted = %s
            where id = %s
            """,
            ("success", 1, 1, source_run_id),
        )

        conn.commit()

        print("Fetch completed successfully.")
        print(f"Source run ID: {source_run_id}")
        print(f"Raw document ID: {raw_document_id}")
        print(f"Saved file: {file_path}")

    except Exception as e:
        cur.execute(
            """
            update source_runs
            set run_finished_at = now(),
                status = %s,
                error_message = %s
            where id = %s
            """,
            ("failed", str(e), source_run_id),
        )
        conn.commit()
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()