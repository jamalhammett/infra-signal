import os
import re
import psycopg2 as psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")


TARGET_KEYWORDS = [
    "data center",
    "datacenter",
    "substation",
    "switchyard",
    "transmission",
    "utility",
    "power",
    "energy",
    "fiber",
    "telecom",
    "hyperscale",
    "cloud",
    "server",
]

EXCLUDED_KEYWORDS = [
    "sidewalk",
    "trail",
    "path",
    "driveway",
    "subdivision",
    "townhome",
    "townhomes",
    "single family",
    "residential",
    "lot ",
    "lots",
    "farm",
    "conservancy",
    "school",
    "church",
    "playground",
    "park",
    "landscape",
    "forest",
    "stormwater",
    "relocation drive",
]

KNOWN_COMPANIES = [
    "STACK",
    "QTS",
    "CyrusOne",
    "Vantage",
    "Digital Realty",
    "CoreSite",
    "Aligned",
    "Equinix",
    "Iron Mountain",
    "Cologix",
    "EdgeCore",
    "Compass",
    "Amazon",
    "AWS",
    "Meta",
    "Google",
    "Microsoft",
    "NOVEC",
    "Dominion",
    "Dominion Energy",
]


def clean_text(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"none", "unknown", "unknown target", "n/a", "na", "null"}:
        return None
    return text


def looks_like_case_number(value):
    if not value:
        return False
    return bool(re.match(r"^[A-Z]{2,10}-\d{4}-\d+", value.strip(), re.I))


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
            "project_description",
            "description",
            "project_type",
            "project_stage",
            "county",
            "state",
            "created_at",
        ]
        if col in project_columns
    ]

    if "id" not in usable_cols:
        raise ValueError("projects table must have an id column")

    select_cols = ", ".join(usable_cols)

    where_parts = []

    if "created_at" in project_columns:
        where_parts.append("created_at >= now() - interval '90 days'")

    if "project_stage" in project_columns:
        where_parts.append(
            """
            lower(project_stage) in (
                'approved',
                'in review',
                'submitted',
                'submitted - online',
                'pending'
            )
            """
        )

    where_sql = "where " + " and ".join(where_parts) if where_parts else ""

    cur.execute(
        f"""
        select {select_cols}
        from projects
        {where_sql}
        order by created_at desc
        limit 1000
        """
    )

    return [dict(zip(usable_cols, row)) for row in cur.fetchall()]


def combined_project_text(project):
    parts = [
        project.get("target_company"),
        project.get("canonical_project_name"),
        project.get("project_name"),
        project.get("project_description"),
        project.get("description"),
        project.get("project_type"),
        project.get("project_stage"),
    ]
    return " ".join([clean_text(p) or "" for p in parts]).strip()


def is_high_value_target(project):
    text = combined_project_text(project)
    lowered = text.lower()

    if not text:
        return False

    if any(bad in lowered for bad in EXCLUDED_KEYWORDS):
        return False

    if not any(good in lowered for good in TARGET_KEYWORDS):
        return False

    return True


def extract_company(project):
    text = combined_project_text(project)
    lowered = text.lower()

    for company in KNOWN_COMPANIES:
        if company.lower() in lowered:
            return company

    raw_target = clean_text(project.get("target_company"))

    if raw_target and not looks_like_case_number(raw_target):
        raw_lower = raw_target.lower()

        if any(bad in raw_lower for bad in EXCLUDED_KEYWORDS):
            return None

        if any(good in lowered for good in TARGET_KEYWORDS):
            return raw_target

    return None


def build_lead(project):
    if not is_high_value_target(project):
        return None

    company = extract_company(project)

    if not company:
        return None

    return {
        "project_id": project.get("id"),
        "case_number": project.get("case_number"),
        "company": company,
        "contact_name": "BD Contact Needed",
        "title": "Infrastructure / Data Center Opportunity Lead",
        "phone": None,
        "email": None,
    }


def lead_exists(cur, leads_columns, lead):
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
            where lower(trim(company)) = lower(trim(%s))
            limit 1
            """,
            (lead["company"],),
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

    insert_fields = [field for field in allowed_fields if field in leads_columns]

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
