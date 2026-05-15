import os
import psycopg2 as psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")


TARGET_KEYWORDS = [
    "data center",
    "substation",
    "switchyard",
    "transmission",
    "utility",
    "industrial",
    "warehouse",
    "hyperscale",
    "campus",
    "fiber",
    "telecom",
    "power",
    "energy",
    "server",
    "cloud",
]


EXCLUDED_KEYWORDS = [
    "sidewalk",
    "residential",
    "townhome",
    "single family",
    "duplex",
    "school",
    "park",
    "church",
    "road widening",
    "landscape",
    "forest",
    "playground",
    "trail",
    "side path",
    "driveway",
    "garage",
]


KNOWN_COMPANIES = [
    "CyrusOne",
    "Vantage",
    "QTS",
    "Digital Realty",
    "STACK",
    "Switch",
    "CoreSite",
    "Aligned",
    "Amazon",
    "Meta",
    "Google",
    "Microsoft",
    "Compass",
    "EdgeCore",
    "Equinix",
    "Iron Mountain",
    "Cologix",
    "NOVEC",
    "Dominion",
    "Sabey",
]


def clean_text(value):
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
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


def get_projects(cur):
    project_columns = get_columns(cur, "projects")

    usable_cols = [
        col for col in [
            "id",
            "case_number",
            "project_name",
            "canonical_project_name",
            "target_company",
            "project_stage",
            "created_at",
        ]
        if col in project_columns
    ]

    select_cols = ", ".join(usable_cols)

    where_clauses = []

    if "created_at" in project_columns:
        where_clauses.append(
            "created_at >= now() - interval '90 days'"
        )

    if "project_stage" in project_columns:
        where_clauses.append(
            """
            lower(project_stage) in (
                'approved',
                'in review',
                'submitted',
                'pending'
            )
            """
        )

    where_sql = ""

    if where_clauses:
        where_sql = "where " + " and ".join(where_clauses)

    query = f"""
        select {select_cols}
        from projects
        {where_sql}
        order by created_at desc
        limit 500
    """

    cur.execute(query)

    rows = cur.fetchall()

    projects = []

    for row in rows:
        item = dict(zip(usable_cols, row))
        projects.append(item)

    return projects


def is_target_project(project_name):
    if not project_name:
        return False

    lowered = project_name.lower()

    if any(keyword in lowered for keyword in EXCLUDED_KEYWORDS):
        return False

    if any(keyword in lowered for keyword in TARGET_KEYWORDS):
        return True

    return False


def extract_company(project_name):
    if not project_name:
        return None

    lowered = project_name.lower()

    for company in KNOWN_COMPANIES:
        if company.lower() in lowered:
            return company

    return None


def build_lead(project):
    project_name = (
        clean_text(project.get("canonical_project_name"))
        or clean_text(project.get("project_name"))
        or clean_text(project.get("target_company"))
    )

    if not project_name:
        return None

    if not is_target_project(project_name):
        return None

    company = extract_company(project_name)

    if not company:
        return None

    return {
        "project_id": project.get("id"),
        "case_number": project.get("case_number"),
        "company": company,
        "contact_name": "BD Contact Needed",
        "title": "Infrastructure Opportunity Lead",
        "phone": None,
        "email": None,
    }


def lead_exists(cur, leads_columns, lead):
    company = lead["company"]

    if "project_id" in leads_columns and lead.get("project_id"):
        cur.execute(
            """
            select 1
            from leads
            where project_id = %s
            limit 1
            """,
            (lead["project_id"],),
        )

    elif "case_number" in leads_columns and lead.get("case_number"):
        cur.execute(
            """
            select 1
            from leads
            where case_number = %s
            limit 1
            """,
            (lead["case_number"],),
        )

    else:
        cur.execute(
            """
            select 1
            from leads
            where lower(company) = lower(%s)
            limit 1
            """,
            (company,),
        )

    return cur.fetchone() is not None


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

    projects = get_projects(cur)

    inserted = 0
    skipped = 0

    for project in projects:
        lead = build_lead(project)

        if not lead:
            skipped += 1
            continue

        if lead_exists(cur, leads_columns, lead):
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
