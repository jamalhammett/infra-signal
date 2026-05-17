import os
import psycopg2 as psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found")


def classify_project(text):
    text = (text or "").lower()

    if any(k in text for k in [
        "data center", "datacenter", "hyperscale", "qts", "cyrusone",
        "equinix", "vantage", "aligned", "stack", "digital realty"
    ]):
        return "Hyperscale Expansion", "Data Center", "Northern Virginia Hyperscale Corridor", "Ashburn", 80, True, False, False

    if any(k in text for k in [
        "substation", "transmission", "switchyard", "dominion",
        "novec", "power", "electric", "energy", "utility"
    ]):
        return "Utility Capacity Expansion", "Utility Infrastructure", "Northern Virginia Power Corridor", "Loudoun", 70, False, True, False

    if any(k in text for k in ["fiber", "telecom", "broadband"]):
        return "Connectivity Expansion", "Fiber Infrastructure", "Northern Virginia Fiber Corridor", "Ashburn", 65, False, False, True

    return "General Infrastructure", "General Infrastructure", "Northern Virginia Infrastructure Region", "Loudoun", 10, False, False, False


def main():
    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        select
            s.id,
            s.case_number,
            s.project_name,
            s.project_description,
            s.applicant_name,
            s.owner_name,
            s.contractor_name,
            s.parcel_id,
            s.address_raw,
            s.county,
            s.state,
            s.project_type,
            s.project_stage,
            s.hearing_date,
            s.estimated_value,
            s.created_at
        from signals s
        where not exists (
            select 1
            from projects p
            where p.case_number = s.case_number
        )
    """)

    rows = cur.fetchall()
    inserted = 0

    for row in rows:
        signal_id = row[0]
        case_number = row[1]
        project_name = row[2]
        description = row[3]
        applicant_name = row[4]
        owner_name = row[5]
        contractor_name = row[6]
        parcel_id = row[7]
        address_raw = row[8]
        county = row[9] or "Loudoun"
        state = row[10] or "VA"
        project_type = row[11]
        project_stage = row[12]
        hearing_date = row[13]
        estimated_value = row[14]
        created_at = row[15]

        combined_text = " ".join([
            str(project_name or ""),
            str(description or ""),
            str(project_type or ""),
            str(applicant_name or ""),
            str(owner_name or ""),
            str(contractor_name or ""),
            str(address_raw or ""),
        ])

        (
            intelligence_category,
            infrastructure_type,
            corridor_region,
            market_cluster,
            score,
            hyperscale_related,
            utility_related,
            fiber_related,
        ) = classify_project(combined_text)

        predictive_signal = score >= 60

        cur.execute("""
            insert into projects (
                signal_id,
                case_number,
                canonical_project_name,
                permit_description,
                applicant_name,
                owner_name,
                contractor_name,
                parcel_id,
                address_raw,
                county,
                state,
                project_type,
                project_stage,
                hearing_date,
                estimated_value,
                intelligence_category,
                infrastructure_type,
                corridor_region,
                market_cluster,
                early_capture_score,
                predictive_signal,
                hyperscale_related,
                utility_related,
                fiber_related,
                source_name,
                source_type,
                raw_text,
                created_at
            )
            values (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s
            )
            on conflict do nothing
        """, (
            signal_id,
            case_number,
            project_name,
            description,
            applicant_name,
            owner_name,
            contractor_name,
            parcel_id,
            address_raw,
            county,
            state,
            project_type,
            project_stage,
            hearing_date,
            estimated_value,
            intelligence_category,
            infrastructure_type,
            corridor_region,
            market_cluster,
            score,
            predictive_signal,
            hyperscale_related,
            utility_related,
            fiber_related,
            "Loudoun County Land Applications",
            "County Planning Feed",
            combined_text,
            created_at,
        ))

        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Promoted {inserted} signals to projects", flush=True)


if __name__ == "__main__":
    main()
