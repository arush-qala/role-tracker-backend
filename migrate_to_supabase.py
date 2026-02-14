"""
One-time migration script: copies data from local SQLite (roles.db) to Supabase PostgreSQL.

Usage:
    1. Set DATABASE_URL in your .env file to your Supabase connection string
    2. Run: python migrate_to_supabase.py
"""
import sqlite3
import json
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SQLITE_PATH = os.path.join(os.path.dirname(__file__), "roles.db")
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL in .env first (your Supabase connection string)")
    exit(1)

if not os.path.exists(SQLITE_PATH):
    print(f"No SQLite database found at {SQLITE_PATH}. Nothing to migrate.")
    exit(0)

# Connect to both databases
sqlite_conn = sqlite3.connect(SQLITE_PATH)
sqlite_conn.row_factory = sqlite3.Row
pg_conn = psycopg2.connect(DATABASE_URL)
pg_cur = pg_conn.cursor()

print("Connected to both databases.")

# --- Migrate companies ---
companies = sqlite_conn.execute("SELECT * FROM companies").fetchall()
print(f"Migrating {len(companies)} companies...")

company_id_map = {}  # old_id -> new_id
for c in companies:
    pg_cur.execute(
        """INSERT INTO companies (name, careers_url, active, created_at, last_scraped_at)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (name) DO UPDATE SET careers_url = EXCLUDED.careers_url
           RETURNING id""",
        (c["name"], c["careers_url"], c["active"], c["created_at"], c["last_scraped_at"])
    )
    new_id = pg_cur.fetchone()[0]
    company_id_map[c["id"]] = new_id

pg_conn.commit()
print(f"  Done. ID mapping: {company_id_map}")

# --- Migrate roles ---
roles = sqlite_conn.execute("SELECT * FROM roles").fetchall()
print(f"Migrating {len(roles)} roles...")

for r in roles:
    new_company_id = company_id_map.get(r["company_id"])
    if not new_company_id:
        print(f"  Skipping role '{r['title']}' - no matching company")
        continue

    pg_cur.execute(
        """INSERT INTO roles (company_id, title, url, location, description, seniority,
           department, posted_date, score, score_breakdown, status, first_seen_at, last_seen_at, applied_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (company_id, title, location) DO NOTHING""",
        (new_company_id, r["title"], r["url"], r["location"], r["description"],
         r["seniority"], r["department"], r["posted_date"], r["score"],
         r["score_breakdown"], r["status"], r["first_seen_at"], r["last_seen_at"],
         r["applied_at"])
    )

pg_conn.commit()
print("  Done.")

# --- Migrate scrape logs ---
logs = sqlite_conn.execute("SELECT * FROM scrape_logs").fetchall()
print(f"Migrating {len(logs)} scrape logs...")

for log in logs:
    new_company_id = company_id_map.get(log["company_id"])
    if not new_company_id:
        continue

    pg_cur.execute(
        """INSERT INTO scrape_logs (company_id, started_at, finished_at, roles_found, roles_qualified, status, error)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (new_company_id, log["started_at"], log["finished_at"], log["roles_found"],
         log["roles_qualified"], log["status"], log["error"])
    )

pg_conn.commit()
print("  Done.")

# Cleanup
sqlite_conn.close()
pg_cur.close()
pg_conn.close()

print("\nMigration complete! Your data is now in Supabase.")
