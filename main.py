import sys
from io import StringIO
from typing import Tuple
from src.config import logger
from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.analytics import Analytics

# =============================================================================
# MOCKED PRODUCTION DATASETS (Simulation)
# =============================================================================

def get_production_mock_data() -> Tuple[StringIO, StringIO]:
    """Generates high-entropy simulated datasets for pipeline validation."""
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

# =============================================================================
# PIPELINE ORCHESTRATION
# =============================================================================

def run_pipeline():
    """Executes the modular ETL pipeline."""
    logger.info("Pipeline Execution Started.")
    
    # 1. Extraction Phase
    coupons_io, rules_io = get_production_mock_data()
    df_valid, df_quarantine = Extractor.extract_coupons(coupons_io)
    df_rules = Extractor.extract_rules(rules_io)
    
    try:
        # 2. Transformation Phase
        df_processed = Transformer.apply_rules_engine(df_valid, df_rules)
        df_final = Transformer.calculate_financials(df_processed)
        
        # 3. Loading Phase
        Loader.load_to_sqlite(df_final, "discount_audit")
        if not df_quarantine.empty:
            Loader.load_to_sqlite(df_quarantine, "quarantine_logs")
        
        # 4. Analytics Phase
        Analytics.generate_impact_report(df_final, df_quarantine)
        
        logger.info("Pipeline Status: SUCCESS")
        
    except Exception as e:
        logger.error(f"Pipeline Status: CRITICAL FAILURE | {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
