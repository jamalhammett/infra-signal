import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL"")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")

def get_companies():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT project_name
        FROM signals
        WHERE created_at >= now() - interval '90 days'
        LIMIT 50
    """)

    companies = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return companies


def generate_fake_leads(company):
    return {
        "contact_name": "Project Director",
        "title": "Director of Construction",
        "phone": "555-123-4567",
        "email": f"contact@{company.replace(' ', '').lower()}.com"
    }


def insert_lead(company, lead):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO leads (company, contact_name, title, phone, email)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        company,
        lead["contact_name"],
        lead["title"],
        lead["phone"],
        lead["email"]
    ))

    conn.commit()
    cur.close()
    conn.close()


def main():
    companies = get_companies()

    for company in companies:
        lead = generate_fake_leads(company)
        insert_lead(company, lead)

    print("✅ Lead enrichment complete.")


if __name__ == "__main__":
    main()
