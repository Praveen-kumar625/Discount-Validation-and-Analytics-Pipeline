# Author: khushboo kaushik
import logging
import sys
from typing import Final

# -----------------------------------------------------------------------------
# PIPELINE CONFIG & LOGGING SETUP
# -----------------------------------------------------------------------------

# This is where we keep our SQLite DB. It's serverless and does the job for our needs.
DB_NAME: Final[str] = "discount_analytics.db"

def setup_logging():
    """
    Sets up a clean, structured logging format so we can actually see 
    what's happening inside the pipeline without digging through junk.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger("DiscountPipeline")

# We initialize this once and use it everywhere.
logger = setup_logging()
