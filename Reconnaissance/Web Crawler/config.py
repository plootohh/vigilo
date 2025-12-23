import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "vigilo.db")
LOG_PATH = os.path.join(BASE_DIR, "data", "vigilo.log")

USER_AGENT = "Vigilo/0.1.2 (Personal research bot; non-commercial) mailme31@proton.me)"

MAX_BYTES = 10_000_000
CRAWL_DELAY = 0.5