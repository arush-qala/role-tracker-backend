import json
import os
import psycopg2
import psycopg2.extras
from datetime import datetime
from config import DATABASE_URL


def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            careers_url TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT NOW(),
            last_scraped_at TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS roles (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id),
            title TEXT NOT NULL,
            url TEXT,
            location TEXT,
            description TEXT,
            seniority TEXT,
            department TEXT,
            posted_date TEXT,
            score INTEGER,
            score_breakdown TEXT,
            status TEXT DEFAULT 'new',
            first_seen_at TIMESTAMP DEFAULT NOW(),
            last_seen_at TIMESTAMP DEFAULT NOW(),
            applied_at TIMESTAMP,
            UNIQUE(company_id, title, location)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scrape_logs (
            id SERIAL PRIMARY KEY,
            company_id INTEGER REFERENCES companies(id),
            started_at TIMESTAMP DEFAULT NOW(),
            finished_at TIMESTAMP,
            roles_found INTEGER DEFAULT 0,
            roles_qualified INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running',
            error TEXT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


def _row_to_dict(cur):
    """Convert cursor results to list of dicts using column names."""
    if cur.description is None:
        return []
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    result = []
    for row in rows:
        d = {}
        for col, val in zip(columns, row):
            if isinstance(val, datetime):
                d[col] = val.isoformat()
            else:
                d[col] = val
        result.append(d)
    return result


def seed_companies(companies_file):
    """Load companies from JSON file into DB if not already present."""
    with open(companies_file, "r") as f:
        companies = json.load(f)

    conn = get_connection()
    cur = conn.cursor()
    for company in companies:
        cur.execute(
            "INSERT INTO companies (name, careers_url, active) VALUES (%s, %s, %s) ON CONFLICT (name) DO NOTHING",
            (company["name"], company["careers_url"], 1 if company.get("active", True) else 0)
        )
    conn.commit()
    cur.close()
    conn.close()


def get_active_companies():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM companies WHERE active = 1")
    rows = _row_to_dict(cur)
    cur.close()
    conn.close()
    return rows


def get_all_companies():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM companies ORDER BY name")
    rows = _row_to_dict(cur)
    cur.close()
    conn.close()
    return rows


def add_company(name, careers_url):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO companies (name, careers_url, active) VALUES (%s, %s, 1) ON CONFLICT (name) DO NOTHING",
        (name, careers_url)
    )
    conn.commit()
    cur.close()
    conn.close()


def remove_company(company_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE companies SET active = 0 WHERE id = %s", (company_id,))
    conn.commit()
    cur.close()
    conn.close()


def upsert_role(company_id, title, url, location, description, seniority, department, score, score_breakdown, posted_date=None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, status FROM roles WHERE company_id = %s AND title = %s AND location = %s",
        (company_id, title, location)
    )
    existing = cur.fetchone()

    now = datetime.utcnow()
    breakdown_json = json.dumps(score_breakdown) if isinstance(score_breakdown, dict) else score_breakdown

    if existing:
        cur.execute(
            """UPDATE roles SET url = %s, description = %s, seniority = %s, department = %s,
               posted_date = %s, score = %s, score_breakdown = %s, last_seen_at = %s WHERE id = %s""",
            (url, description, seniority, department, posted_date, score,
             breakdown_json, now, existing[0])
        )
    else:
        cur.execute(
            """INSERT INTO roles (company_id, title, url, location, description, seniority,
               department, posted_date, score, score_breakdown, first_seen_at, last_seen_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (company_id, title, url, location, description, seniority, department, posted_date,
             score, breakdown_json, now, now)
        )

    conn.commit()
    cur.close()
    conn.close()


def get_qualified_roles(threshold=80):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT r.*, c.name as company_name, c.careers_url
           FROM roles r JOIN companies c ON r.company_id = c.id
           WHERE r.score >= %s ORDER BY r.score DESC, r.last_seen_at DESC""",
        (threshold,)
    )
    rows = _row_to_dict(cur)
    cur.close()
    conn.close()
    return rows


def get_all_roles():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT r.*, c.name as company_name, c.careers_url
           FROM roles r JOIN companies c ON r.company_id = c.id
           ORDER BY r.score DESC, r.last_seen_at DESC"""
    )
    rows = _row_to_dict(cur)
    cur.close()
    conn.close()
    return rows


def get_roles_by_company(company_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT r.*, c.name as company_name
           FROM roles r JOIN companies c ON r.company_id = c.id
           WHERE r.company_id = %s ORDER BY r.score DESC""",
        (company_id,)
    )
    rows = _row_to_dict(cur)
    cur.close()
    conn.close()
    return rows


def mark_role_applied(role_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE roles SET status = 'applied', applied_at = NOW() WHERE id = %s",
        (role_id,)
    )
    conn.commit()
    cur.close()
    conn.close()


def mark_role_dismissed(role_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE roles SET status = 'dismissed' WHERE id = %s", (role_id,))
    conn.commit()
    cur.close()
    conn.close()


def update_company_scraped(company_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE companies SET last_scraped_at = NOW() WHERE id = %s",
        (company_id,)
    )
    conn.commit()
    cur.close()
    conn.close()


def log_scrape(company_id, roles_found=0, roles_qualified=0, status="completed", error=None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO scrape_logs (company_id, finished_at, roles_found, roles_qualified, status, error)
           VALUES (%s, NOW(), %s, %s, %s, %s)""",
        (company_id, roles_found, roles_qualified, status, error)
    )
    conn.commit()
    cur.close()
    conn.close()


def get_scrape_history(limit=20):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT sl.*, c.name as company_name FROM scrape_logs sl
           JOIN companies c ON sl.company_id = c.id
           ORDER BY sl.started_at DESC LIMIT %s""",
        (limit,)
    )
    rows = _row_to_dict(cur)
    cur.close()
    conn.close()
    return rows


def get_dashboard_stats():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM companies WHERE active = 1")
    total_companies = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM roles")
    total_roles = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM roles WHERE score >= 80")
    qualified_roles = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM roles WHERE status = 'applied'")
    applied_roles = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM roles WHERE status = 'new' AND score >= 80")
    new_roles = cur.fetchone()[0]
    cur.close()
    conn.close()
    return {
        "total_companies": total_companies,
        "total_roles": total_roles,
        "qualified_roles": qualified_roles,
        "applied_roles": applied_roles,
        "new_roles": new_roles,
    }
