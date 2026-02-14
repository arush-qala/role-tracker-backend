from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from config import SCRAPE_HOUR, SCRAPE_MINUTE
import db
from scraper import scrape_company_roles
from scorer import score_roles_batch
from config import SCORE_THRESHOLD
import logging

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()


def scheduled_scrape():
    """Run a full scrape of all active companies. Called by the scheduler."""
    logger.info("Scheduled scrape starting...")
    companies = db.get_active_companies()

    for company in companies:
        try:
            logger.info(f"Scraping {company['name']}...")
            roles = scrape_company_roles(company["name"], company["careers_url"])
            logger.info(f"Found {len(roles)} roles at {company['name']}, scoring...")

            scored = score_roles_batch(roles, company["name"])
            qualified_count = 0

            for role, score_result in scored:
                total_score = score_result.get("total_score", 0)
                if total_score >= SCORE_THRESHOLD:
                    qualified_count += 1

                db.upsert_role(
                    company_id=company["id"],
                    title=role.get("title", "Unknown"),
                    url=role.get("url", ""),
                    location=role.get("location", ""),
                    description=role.get("description", ""),
                    seniority=role.get("seniority", ""),
                    department=role.get("department", ""),
                    score=total_score,
                    score_breakdown=score_result.get("breakdown", {})
                )

            db.update_company_scraped(company["id"])
            db.log_scrape(company["id"], len(roles), qualified_count, "completed")
            logger.info(f"Done: {company['name']} - {qualified_count}/{len(roles)} qualified")

        except Exception as e:
            logger.error(f"Error scraping {company['name']}: {e}")
            db.log_scrape(company["id"], 0, 0, "error", str(e))

    logger.info("Scheduled scrape complete.")


def start_scheduler():
    """Start the daily scrape scheduler."""
    scheduler.add_job(
        scheduled_scrape,
        trigger=CronTrigger(hour=SCRAPE_HOUR, minute=SCRAPE_MINUTE),
        id="daily_scrape",
        replace_existing=True
    )
    scheduler.start()
    logger.info(f"Scheduler started. Daily scrape at {SCRAPE_HOUR:02d}:{SCRAPE_MINUTE:02d}")


def stop_scheduler():
    scheduler.shutdown()
