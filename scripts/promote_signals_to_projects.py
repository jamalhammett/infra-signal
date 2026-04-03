import os

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL was not found in .env")

BATCH_SIZE = 50


def score_project(project_type: str | None, project_stage: str | None, description: str | None) -> int:
    score = 0
    text = (description or "").lower()
    ptype = (project_type or "").lower()
    pstage = (project_stage or "").lower()

    if "data center" in text:
        score += 50
    if "warehouse" in text:
        score += 30
    if "commercial" in text:
        score += 20
    if "rezoning" in ptype or "rezoning" in text:
        score += 15
    if "site plan" in ptype:
        score += 10
    if "approved" in pstage:
        score += 10
    if "in review" in pstage:
        score += 5

    return score


def get_connection():
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=20,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def main():
    total_inserted = 0

    while True:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("set statement_timeout = 0")

                cur.execute(
                    """
                    select
                        s.case_number,
                        s.project_name,
                        s.project_description,
                        s.county,
                        s.state,
                        s.project_type,
                        s.project_stage
                    from signals s
                    left join projects p
                        on p.case_number = s.case_number
                    where p.case_number is null
                      and s.case_number is not null
                    order by s.created_at, s.case_number
                    limit %s
                    """,
                    (BATCH_SIZE,)
                )

                rows = cur.fetchall()

                if not rows:
                    print("No more new signals to promote.", flush=True)
                    break

                for row in rows:
                    case_number, project_name, description, county, state, project_type, project_stage = row

                    if project_name and "test" not in project_name.lower():
                        canonical_project_name = project_name
                    else:
                        canonical_project_name = case_number

                    opportunity_score = score_project(project_type, project_stage, description)

                    cur.execute(
                        """
                        insert into projects (
                            case_number,
                            canonical_project_name,
                            county,
                            state,
                            project_type,
                            project_stage,
                            confidence_score,
                            opportunity_score,
                            first_seen_at,
                            last_seen_at,
                            created_at,
                            description
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now(), now(), %s)
                        on conflict (case_number) do update
                        set
                            canonical_project_name = excluded.canonical_project_name,
                            county = excluded.county,
                            state = excluded.state,
                            project_type = excluded.project_type,
                            project_stage = excluded.project_stage,
                            confidence_score = excluded.confidence_score,
                            opportunity_score = excluded.opportunity_score,
                            last_seen_at = now(),
                            description = excluded.description
                        """,
                        (
                            case_number,
                            canonical_project_name,
                            county,
                            state,
                            project_type,
                            project_stage,
                            90,
                            opportunity_score,
                            description,
                        )
                    )

                conn.commit()

                total_inserted += len(rows)
                print(
                    f"Promoted batch of {len(rows)} | Total promoted this run: {total_inserted}",
                    flush=True
                )

    print(f"Finished. Total promoted this run: {total_inserted}", flush=True)


if __name__ == "__main__":
    main()