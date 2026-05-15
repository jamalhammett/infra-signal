import os
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
    "Loudoun Business Park",
    "Greenlin Park",
]


def clean_text(value):
    if value is None:
        return None

    text = str(value).strip()

    if text.lower() in BAD_VALUES:
        return None

    return text


def get_columns(cur, table_name):
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


def pick_project_name(row):
    for key in [
        "target_company",
        "canonical_project_name",
        "project_name",
        "name",
        "title",
    ]:
        value = clean_text(row.get(key))
        if value:
            return value

    return None


def extract_company(project_name):
    project_name = clean_text(project_name)

    if not project_name:
        return None

    lowered = project_name.lower()

    for company in KNOWN_COMPANIES:
        if company.lower() in lowered:
            return company

    junk_terms = [
        "gis update",
        "agricultural",
        "forestal",
        "vineyard",
        "unknown",
        "none",
    ]

    if any(term in lowered for term in junk_terms):
        return None

    if len(project_name) < 4:
        return None

    return project_name


def get_projects(cur):
    project_columns = get_columns(cur, "projects")

    if "id" not in project_columns:
        raise ValueError("projects table must have an id column")

    usable_cols = [
        col for col in [
            "id",
            "case_number",
            "target_company",
            "canonical_project_name",
            "project_name",
            "name",
            "title",
            "created_at",
        ]
        if col in project_columns
    ]

    select_cols = ", ".join(usable_cols)
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
    projects = []

    for row in rows:
        item = dict(zip(usable_cols, row))

        project_name = pick_project_name(item)
        company = extract_company(project_name)

        if not company:
            continue

        projects.append(
            {
                "project_id": item.get("id"),
                "case_number": item.get("case_number"),
                "company": company,
            }
        )

    return projects


def lead_exists(cur, leads_columns, project):
    company = project["company"]

    if "project_id" in leads_columns:
        cur.execute(
            """
            select 1
            from leads
            where project_id = %s
              and lower(trim(company)) = lower(trim(%s))
            limit 1
            """,
            (project["project_id"], company),
        )

    elif "case_number" in leads_columns and project.get("case_number"):
        cur.execute(
            """
            select 1
            from leads
            where case_number = %s
              and lower(trim(company)) = lower(trim(%s))
            limit 1
            """,
            (project["case_number"], company),
        )

    else:
        cur.execute(
            """
            select 1
            from leads
            where lower(trim(company)) = lower(trim(%s))
            limit 1
            """,
            (company,),
        )

    return cur.fetchone() is not None


def build_lead(project):
    company = clean_text(project.get("company"))

    if not company:
        return None

    return {
        "project_id": project.get("project_id"),
        "case_number": project.get("case_number"),
        "company": company,
        "contact_name": "BD Contact Needed",
        "title": "Project / Business Development Lead",
        "phone": None,
        "email": None,
    }


def insert_lead(cur, leads_columns, lead):
    allowed_fields = [
        "project_id",
        "case_number",
        "company",
        "contact_name",
        "title",
        "phone",
        "email",
    ]

    insert_fields = [
        field for field in allowed_fields
        if field in leads_columns
    ]

    if "company" not in insert_fields:
        raise ValueError("leads table must have a company column")

    values = [lead.get(field) for field in insert_fields]

    columns_sql = ", ".join(insert_fields)
    placeholders = ", ".join(["%s"] * len(insert_fields))

    if "created_at" in leads_columns:
        columns_sql += ", created_at"
        placeholders += ", now()"

    cur.execute(
        f"""
        insert into leads ({columns_sql})
        values ({placeholders})
        """,
        values,
    )


def main():
    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    leads_columns = get_columns(cur, "leads")

    if "company" not in leads_columns:
        raise ValueError("leads table must have a company column")

    projects = get_projects(cur)

    inserted = 0
    skipped = 0

    for project in projects:
        lead = build_lead(project)

        if not lead:
            skipped += 1
            continue

        if lead_exists(cur, leads_columns, project):
            skipped += 1
            continue

        insert_lead(cur, leads_columns, lead)
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Projects reviewed: {len(projects)}", flush=True)
    print(f"Inserted leads: {inserted}", flush=True)
    print(f"Skipped records: {skipped}", flush=True)


if __name__ == "__main__":
    main()
