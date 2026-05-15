import os
import psycopg2 as psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")


def clean_text(value):
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    if text.lower() in ["none", "unknown", "unknown target", "unknown company", "n/a", "na"]:
        return None

    return text


def make_placeholder_lead(company):
    company = clean_text(company)

    if not company:
        return None

    return {
        "company": company,
        "contact_name": "BD Contact Needed",
        "title": "Project / Business Development Lead",
        "phone": None,
        "email": None,
    }


def get_projects():
    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute(
        """
        select
            id,
            target_company
        from projects
        where target_company is not null
          and trim(target_company) <> ''
          and lower(trim(target_company)) not in (
              'none',
              'unknown',
              'unknown target',
              'unknown company',
              'n/a',
              'na'
          )
        order by created_at desc
        limit 500
        """
    )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


def lead_exists(cur, project_id, company):
    cur.execute(
        """
        select id
        from leads
        where project_id = %s
          and lower(trim(company)) = lower(trim(%s))
        limit 1
        """,
        (project_id, company),
    )

    return cur.fetchone() is not None


def insert_lead(cur, project_id, lead):
    cur.execute(
        """
        insert into leads (
            project_id,
            company,
            contact_name,
            title,
            phone,
            email,
            created_at
        )
        values (%s, %s, %s, %s, %s, %s, now())
        """,
        (
            project_id,
            lead["company"],
            lead["contact_name"],
            lead["title"],
            lead["phone"],
            lead["email"],
        ),
    )


def main():
    projects = get_projects()

    inserted = 0
    skipped = 0

    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    for project_id, company in projects:
        company = clean_text(company)

        if not company:
            skipped += 1
            continue

        lead = make_placeholder_lead(company)

        if not lead:
            skipped += 1
            continue

        if lead_exists(cur, project_id, company):
            skipped += 1
            continue

        insert_lead(cur, project_id, lead)
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted leads: {inserted}", flush=True)
    print(f"Skipped records: {skipped}", flush=True)


if __name__ == "__main__":
    main()
