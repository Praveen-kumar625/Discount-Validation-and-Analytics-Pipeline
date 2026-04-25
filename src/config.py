import logging
import sys
from typing import Final

# =============================================================================
# INFRASTRUCTURE & LOGGING CONFIGURATION
# =============================================================================

DB_NAME: Final[str] = "discount_analytics.db"

def setup_logging():
    """Configures structured logging for the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger("DiscountPipeline")

logger = setup_logging()
