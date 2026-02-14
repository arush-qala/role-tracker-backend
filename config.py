import os
from dotenv import load_dotenv

load_dotenv()

PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# PostgreSQL connection string (Supabase)
DATABASE_URL = os.getenv("DATABASE_URL")

# Scoring threshold - only surface roles at or above this score
SCORE_THRESHOLD = 80

# Target locations (priority order)
TARGET_LOCATIONS = ["London"]

# Scrape schedule - daily at 8am
SCRAPE_HOUR = 8
SCRAPE_MINUTE = 0

# Server port (Render sets PORT env var)
PORT = int(os.getenv("PORT", 8000))
