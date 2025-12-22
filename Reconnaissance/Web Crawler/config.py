import os

# get the directory where config.py lives
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# define paths relative to the root
DB_PATH = os.path.join(BASE_DIR, "data", "vigilo.db")
LOG_PATH = os.path.join(BASE_DIR, "data", "vigilo.log")

# crawler settings
USER_AGENT = "Vigilo/0.1 (Personal research, non-commercial) mailme31@proton.me"
MAX_BYTES = 5_000_000
CRAWL_DELAY = 1.0