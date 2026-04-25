# Author: khushboo kaushik
import pandas as pd
from io import StringIO
from typing import Tuple
from src.config import logger
from src.schemas import COUPON_DATA_SCHEMA, RULES_DATA_SCHEMA

class Extractor:
    """
    This class is all about getting data into our system and making 
    sure we aren't processing junk. We split it into 'good data' and 
    'the quarantine pile'.
    """

    @staticmethod
    def extract_coupons(coupons_io: StringIO) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        We take the raw coupon stream, cast the types correctly, and 
        identify malformed records that should be pushed to the DLQ.
        """
        logger.info("Extracting coupons and checking for structural issues...")
        
        # Grab the raw data
        df = pd.read_csv(coupons_io)
        
        # Try to cast prices and dates; errors become NaN so we can catch them in the mask
        df['original_price'] = pd.to_numeric(df['original_price'], errors='coerce')
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        
        # We define 'trash' as anything with missing IDs, missing/negative prices, or invalid dates.
        dlq_mask = (
            df['original_price'].isna() | 
            (df['original_price'] < 0) | 
            df['order_date'].isna() |
            df['order_id'].isna()
        )
        
        quarantine = df[dlq_mask].copy()
        valid_data = df[~dlq_mask].copy()
        
        # Final cleanup: make sure the valid data types match our schema perfectly
        for col, dtype in COUPON_DATA_SCHEMA.items():
            if col in valid_data.columns and dtype != 'datetime64[ns]':
                valid_data[col] = valid_data[col].astype(dtype)

        if not quarantine.empty:
            logger.warning(f"Heads up! Found {len(quarantine)} malformed records. Sending them to quarantine.")
            
        return valid_data, quarantine

    @staticmethod
    def extract_rules(rules_io: StringIO) -> pd.DataFrame:
        """
        Ingests the coupon rulebook. We're pretty strict with the types here 
        since the whole rules engine depends on this being correct.
        """
        logger.info("Extracting the rulebook...")
        df = pd.read_csv(rules_io)
        
        # Ensure dates are actually dates
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], errors='coerce')
        
        # Enforce numeric types where they matter
        for col, dtype in RULES_DATA_SCHEMA.items():
            if col in df.columns and dtype != 'datetime64[ns]':
                df[col] = pd.to_numeric(df[col], errors='coerce') if dtype == 'float64' else df[col].astype(dtype)
                
        return df
