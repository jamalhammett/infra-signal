import os
import re
import psycopg2 as psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")


BAD_VALUES = {
    "",
    "none",
    "unknown",
    "unknown target",
    "unknown company",
    "n/a",
    "na",
    "null",
}


KNOWN_COMPANIES = [
    "CyrusOne",
    "Vantage Data Centers",
    "Vantage",
    "QTS",
    "Digital Realty",
    "STACK",
    "Switch",
    "CoreSite",
    "Aligned",
    "NOVEC",
    "Dominion Energy",
    "BlackChamber Group",
    "Intergate",
    "LC3",
    "Amazon",
    "Meta",
    "Google",
    "Microsoft",
    "Compass",
    "EdgeCore",
    "Equinix",
    "Iron Mountain",
    "Cologix",
]


def clean_text(value):
    if value is None:
        return None

    text = str(value).strip()

    if text.lower() in BAD_VALUES:
        return None

    return text


def get_table_columns(cur, table_name):
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
        """,
        (table_name,),
    )
    return {row[0] for row in cur.fetchall()}


def pick_project_name_column(project_columns):
    for col in [
        "target_company",
        "canonical_project_name",
        "project_name",
        "name",
        "title",
    ]:
        if col in project_columns:
            return col

    raise ValueError(
        "No usable project name/company column found in projects table."
    )


def extract_company(project_name):
    project_name = clean_text(project_name)

    if not project_name:
        return None

    lowered = project_name.lower()

    for company in KNOWN_COMPANIES:
        if company.lower() in lowered:
            return company

    # Use project name as company only if it is meaningful
    junk_terms = [
        "gis update",
        "unknown",
        "none",
        "agricultural",
        "forestal",
        "vineyard",
    ]

    if any(term in lowered for term in junk_terms):
        return None

    if len(project_name) < 4:
        return None

    return project_name


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


def get_projects(cur):
    project_columns = get_table_columns(cur, "projects")
    company_source_col = pick_project_name_column(project_columns)

    optional_cols = []

    for col in ["id", "case_number", company_source_col, "created_at"]:
        if col in project_columns and col not in optional_cols:
            optional_cols.append(col)

    select_cols = ", ".join(optional_cols)

    order_clause = "order by created_at desc" if "created_at" in project_columns else ""

    cur.execute(
        f"""
        select {select_cols}
        from projects
        {order_clause}
        limit 500
        """
    )

    rows = cur.fetchall()
    col_index = {col: idx for idx, col in enumerate(optional_cols)}

    projects = []

    for row in rows:
        project_id = row[col_index["id"]]
        project_name = row[col_index[company_source_col]]

        company = extract_company(project_name)

        if not company:
            continue

        projects.append(
            {
                "project_id": project_id,
                "company": company,
            }
        )

    return projects


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
    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    projects = get_projects(cur)

    inserted = 0
    skipped = 0

    for project in projects:
        project_id = project["project_id"]
        company = clean_text(project["company"])

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

    print(f"Projects reviewed: {len(projects)}", flush=True)
    print(f"Inserted leads: {inserted}", flush=True)
    print(f"Skipped records: {skipped}", flush=True)


if __name__ == "__main__":
    main()
