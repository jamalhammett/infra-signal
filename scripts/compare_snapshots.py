import os
import psycopg
from dotenv import load_dotenv
from pathlib import Path
import difflib

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def main():

    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
    select storage_path
    from raw_documents
    order by fetched_at desc
    limit 2
    """)

    rows = cur.fetchall()

    if len(rows) < 2:
        print("Not enough snapshots yet.")
        return

    newest = Path(rows[0][0])
    previous = Path(rows[1][0])

    print("Comparing:")
    print(newest)
    print(previous)

    new_text = newest.read_text(encoding="utf-8")
    old_text = previous.read_text(encoding="utf-8")

    diff = list(difflib.unified_diff(
        old_text.splitlines(),
        new_text.splitlines(),
        lineterm=""
    ))

    if not diff:
        print("No changes detected.")
        return

    print("Changes detected:")
    print("\n".join(diff[:200]))


if __name__ == "__main__":
    main()