import os
import psycopg2 as psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found")


KEYWORDS = {
    "data_center": [
        "data center",
        "datacenter",
        "hyperscale",
        "server farm",
        "digital realty",
        "qts",
        "cyrusone",
        "equinix",
        "vantage",
        "aligned",
        "stack",
    ],

    "utility": [
        "substation",
        "transmission",
        "switchyard",
        "dominion",
        "novec",
        "power",
        "electric",
        "energy",
        "utility",
    ],

    "fiber": [
        "fiber",
        "telecom",
        "broadband",
        "dark fiber",
    ],
}


def classify_project(text):

    text = (text or "").lower()

    if any(k in text for k in KEYWORDS["data_center"]):
        return (
            "Hyperscale Expansion",
            "Data Center",
            "Northern Virginia Hyperscale Corridor",
            "Ashburn",
            80,
            True,
            False,
            False,
        )

    if any(k in text for k in KEYWORDS["utility"]):
        return (
            "Utility Capacity Expansion",
            "Utility Infrastructure",
            "Northern Virginia Power Corridor",
            "Loudoun",
            70,
            False,
            True,
            False,
        )

    if any(k in text for k in KEYWORDS["fiber"]):
        return (
            "Connectivity Expansion",
            "Fiber Infrastructure",
            "Northern Virginia Fiber Corridor",
            "Ashburn",
            65,
            False,
            False,
            True,
        )

    return (
        "General Infrastructure",
        "General Infrastructure",
        "Northern Virginia Infrastructure Region",
        "Loudoun",
        10,
        False,
        False,
        False,
    )


def main():

    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute(
        """
        select
            id,
            case_number,
            project_name,
            project_description,
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
            latitude,
            longitude,
            created_at

        from signals

        where promoted_to_project is distinct from true
        """
    )

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
        county = row[9]
        state = row[10]
        project_type = row[11]
        project_stage = row[12]
        hearing_date = row[13]
        estimated_value = row[14]
        latitude = row[15]
        longitude = row[16]
        created_at = row[17]

        combined_text = " ".join(
            [
                str(project_name or ""),
                str(description or ""),
                str(project_type or ""),
            ]
        )

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

        cur.execute(
            """
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

                latitude,
                longitude,

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
                %s, %s,
                %s,
                %s

            )

            on conflict do nothing
            """,
            (
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

                latitude,
                longitude,

                "Loudoun County Land Applications",
                "County Planning Feed",

                combined_text,

                created_at,
            ),
        )

        cur.execute(
            """
            update signals
            set promoted_to_project = true
            where id = %s
            """,
            (signal_id,),
        )

        inserted += 1

    conn.commit()

    cur.close()
    conn.close()

    print(f"Promoted {inserted} signals to projects", flush=True)


if __name__ == "__main__":
    main()
