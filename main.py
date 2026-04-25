# Author: khushboo kaushik
import sys
from io import StringIO
from typing import Tuple
from src.config import logger
from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.analytics import Analytics

# -----------------------------------------------------------------------------
# SIMULATED PRODUCTION DATA
# -----------------------------------------------------------------------------

def get_production_mock_data() -> Tuple[StringIO, StringIO]:
    """
    We're simulating some real-world data here with a bunch of messy edge cases.
    Think conflicts, expired coupons, and malformed prices to stress-test the DLQ.
    """
    applied_coupons = """order_id,product_id,product_category,original_price,coupon_code,order_date
ORD_001,P_EL_01,Electronics,1000.0,SAVE10,2026-04-15 10:00:00
ORD_001,P_EL_01,Electronics,1000.0,SUMMER20,2026-04-15 10:00:00
ORD_002,P_CL_02,Clothing,100.0,CLOTH5,2026-04-10 12:00:00
ORD_003,P_HM_03,Home,200.0,EXPIRED10,2026-04-18 09:00:00
ORD_004,P_EL_04,Electronics,500.0,WRONG_CAT,2026-04-12 15:30:00
ORD_005,P_EL_05,Electronics,-50.0,SAVE10,2026-04-15 11:00:00
ORD_006,P_EL_06,Electronics,0.0,SAVE10,2026-04-15 11:00:00
ORD_007,P_HM_07,Home,300.0,DUPE1,2026-04-15 11:00:00
ORD_007,P_HM_07,Home,300.0,DUPE1,2026-04-15 11:00:00
ORD_008,P_BT_08,Beauty,150.0,MAX_CAP,2026-04-15 11:00:00
ORD_009,P_HM_09,Home,,SAVE10,2026-04-15 11:00:00
"""
    coupon_rules = """coupon_code,expiry_date,applicable_scope,discount_type,discount_value,max_cap,is_exclusive
SAVE10,2026-12-31 23:59:59,Electronics,percentage,10.0,50.0,1
SUMMER20,2026-08-31 23:59:59,Electronics,percentage,20.0,150.0,1
CLOTH5,2026-05-31 23:59:59,Clothing,fixed,5.0,,1
EXPIRED10,2026-03-31 23:59:59,Home,percentage,10.0,20.0,1
WRONG_CAT,2026-12-31 23:59:59,Books,percentage,15.0,30.0,1
DUPE1,2026-12-31 23:59:59,Home,fixed,10.0,,1
MAX_CAP,2026-12-31 23:59:59,Beauty,percentage,50.0,20.0,1
"""
    return StringIO(applied_coupons), StringIO(coupon_rules)

# -----------------------------------------------------------------------------
# MAIN ORCHESTRATION LOOP
# -----------------------------------------------------------------------------

def run_pipeline():
    """
    This is the heart of the ETL. We walk through extracting the data,
    running it through our vectorized rules engine, and finally dumping 
    everything into SQLite for audit and analytics.
    """
    logger.info("Starting the pipeline run...")
    
    # First, let's grab the data and split the good stuff from the trash (quarantine)
    coupons_io, rules_io = get_production_mock_data()
    df_valid, df_quarantine = Extractor.extract_coupons(coupons_io)
    df_rules = Extractor.extract_rules(rules_io)
    
    try:
        # Now we process the valid records through our business logic
        logger.info("Applying business rules and resolving conflicts...")
        df_processed = Transformer.apply_rules_engine(df_valid, df_rules)
        df_final = Transformer.calculate_financials(df_processed)
        
        # Persistence layer: we store accepted/rejected audit logs and the quarantine pile
        Loader.load_to_sqlite(df_final, "discount_audit")
        if not df_quarantine.empty:
            Loader.load_to_sqlite(df_quarantine, "quarantine_logs")
        
        # Finally, let's see what the business impact looks like
        Analytics.generate_impact_report(df_final, df_quarantine)
        
        logger.info("Pipeline wrapped up successfully!")
        
    except Exception as e:
        # If something breaks here, we want the full stack trace so we can fix it fast
        logger.error(f"The pipeline crashed. Here's why: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
