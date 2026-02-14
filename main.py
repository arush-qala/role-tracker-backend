import os
import sys
import logging
import uvicorn
from contextlib import asynccontextmanager

# Ensure backend directory is on the path
sys.path.insert(0, os.path.dirname(__file__))

from db import init_db, seed_companies
from scheduler import start_scheduler, stop_scheduler
from api import app
from config import PORT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


# Run init once at module level (safe across reloads)
logger.info("Initializing database...")
init_db()

companies_file = os.path.join(os.path.dirname(__file__), "companies.json")
if os.path.exists(companies_file):
    seed_companies(companies_file)
    logger.info("Companies seeded from companies.json")

# Only start scheduler in the worker process, not the reloader
if os.environ.get("_ROLE_TRACKER_SCHEDULER") != "1":
    os.environ["_ROLE_TRACKER_SCHEDULER"] = "1"
    logger.info("Starting scheduler...")
    start_scheduler()


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        reload=os.getenv("RENDER") is None  # Only reload locally, not on Render
    )
