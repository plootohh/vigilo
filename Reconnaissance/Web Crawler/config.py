import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "vigilo.db")
LOG_PATH = os.path.join(BASE_DIR, "data", "vigilo.log")

# Identity
USER_AGENT = "Mozilla/5.0 (compatible; Vigilo/0.2; +mailto:mailme31@proton.me)"

# Limits
MAX_BYTES = 5_000_000
CRAWL_DELAY = 0.5

DB_TIMEOUT = 30