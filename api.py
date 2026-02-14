import json
import os
import threading
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

import db
from scraper import scrape_company_roles
from scorer import score_roles_batch
from config import SCORE_THRESHOLD

app = FastAPI(title="Role Tracker API", version="1.0.0")

# Allow Lovable frontend (and localhost dev) to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Track scrape status in memory
_scrape_status = {"is_running": False, "current_company": None, "progress": ""}


class CompanyCreate(BaseModel):
    name: str
    careers_url: str


class RoleStatusUpdate(BaseModel):
    status: str  # "applied" or "dismissed"


# --- Dashboard ---

@app.get("/api/stats")
def get_stats():
    return db.get_dashboard_stats()


# --- Companies ---

@app.get("/api/companies")
def list_companies():
    companies = db.get_all_companies()
    for c in companies:
        if "last_scraped_at" in c:
            c["last_scraped"] = c.pop("last_scraped_at")
    return companies


@app.post("/api/companies")
def add_company(company: CompanyCreate):
    db.add_company(company.name, company.careers_url)
    return {"message": f"Added {company.name}"}


@app.delete("/api/companies/{company_id}")
def deactivate_company(company_id: int):
    db.remove_company(company_id)
    return {"message": "Company deactivated"}


# --- Roles ---

def _normalize_role(role):
    """Rename DB fields to match frontend interface."""
    if "company_name" in role:
        role["company"] = role.pop("company_name")
    if "first_seen_at" in role:
        role["created_at"] = role.pop("first_seen_at")
    # Parse score_breakdown from JSON string if needed
    if isinstance(role.get("score_breakdown"), str):
        try:
            role["score_breakdown"] = json.loads(role["score_breakdown"])
        except (json.JSONDecodeError, TypeError):
            role["score_breakdown"] = {}
    return role


def _normalize_history(entry):
    """Rename DB fields to match frontend interface."""
    if "company_name" in entry:
        entry["company"] = entry.pop("company_name")
    if "started_at" in entry:
        entry["date"] = entry.pop("started_at")
    if "error" in entry:
        entry["error_message"] = entry.pop("error")
    return entry


@app.get("/api/roles")
def list_roles(qualified_only: bool = True, company_id: Optional[int] = None):
    if company_id:
        roles = db.get_roles_by_company(company_id)
    elif qualified_only:
        roles = db.get_qualified_roles(SCORE_THRESHOLD)
    else:
        roles = db.get_all_roles()
    return [_normalize_role(r) for r in roles]


@app.patch("/api/roles/{role_id}")
def update_role_status(role_id: int, update: RoleStatusUpdate):
    if update.status == "applied":
        db.mark_role_applied(role_id)
    elif update.status == "dismissed":
        db.mark_role_dismissed(role_id)
    else:
        raise HTTPException(status_code=400, detail="Status must be 'applied' or 'dismissed'")
    return {"message": f"Role {role_id} marked as {update.status}"}


# --- Scraping ---

def _run_scrape(company_ids=None):
    """Background scrape task."""
    global _scrape_status
    _scrape_status["is_running"] = True

    companies = db.get_active_companies()
    if company_ids:
        companies = [c for c in companies if c["id"] in company_ids]

    for company in companies:
        _scrape_status["current_company"] = company["name"]
        _scrape_status["progress"] = f"Scraping {company['name']}..."

        try:
            roles = scrape_company_roles(company["name"], company["careers_url"])
            _scrape_status["progress"] = f"Scoring {len(roles)} roles from {company['name']}..."

            scored = score_roles_batch(roles, company["name"])
            qualified_count = 0

            for role, score_result in scored:
                total_score = score_result.get("total_score", 0)
                if total_score >= SCORE_THRESHOLD:
                    qualified_count += 1

                raw_posted = role.get("posted_date")
                posted_date = raw_posted if raw_posted and raw_posted != "Not specified" else None

                db.upsert_role(
                    company_id=company["id"],
                    title=role.get("title", "Unknown"),
                    url=role.get("url", ""),
                    location=role.get("location", ""),
                    description=role.get("description", ""),
                    seniority=role.get("seniority", ""),
                    department=role.get("department", ""),
                    score=total_score,
                    score_breakdown=score_result.get("breakdown", {}),
                    posted_date=posted_date
                )

            db.update_company_scraped(company["id"])
            db.log_scrape(company["id"], len(roles), qualified_count, "completed")

        except Exception as e:
            db.log_scrape(company["id"], 0, 0, "error", str(e))

    _scrape_status = {"is_running": False, "current_company": None, "progress": "Done"}


@app.post("/api/scrape")
def trigger_scrape(background_tasks: BackgroundTasks, company_id: Optional[int] = None):
    if _scrape_status["is_running"]:
        raise HTTPException(status_code=409, detail="Scrape already in progress")

    company_ids = [company_id] if company_id else None
    background_tasks.add_task(_run_scrape, company_ids)
    return {"message": "Scrape started", "status": "running"}


@app.get("/api/scrape/status")
def scrape_status():
    return _scrape_status


@app.get("/api/scrape/history")
def scrape_history():
    return [_normalize_history(e) for e in db.get_scrape_history()]
