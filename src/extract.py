import pandas as pd
from io import StringIO
from typing import Tuple
from src.config import logger
from src.schemas import COUPON_DATA_SCHEMA, RULES_DATA_SCHEMA

class Extractor:
    """Handles data ingestion and structural validation (DLQ routing)."""

    @staticmethod
    def extract_coupons(coupons_io: StringIO) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Ingests coupons and separates malformed records into a quarantine DataFrame."""
        logger.info("Extracting applied coupons and validating integrity...")
        
        # Initial read
        df = pd.read_csv(coupons_io)
        
        # Cast types according to schema
        df['original_price'] = pd.to_numeric(df['original_price'], errors='coerce')
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        
        # Define Malformed criteria
        dlq_mask = (
            df['original_price'].isna() | 
            (df['original_price'] < 0) | 
            df['order_date'].isna() |
            df['order_id'].isna()
        )
        
        quarantine = df[dlq_mask].copy()
        valid_data = df[~dlq_mask].copy()
        
        # Final type enforcement for valid data
        for col, dtype in COUPON_DATA_SCHEMA.items():
            if col in valid_data.columns and dtype != 'datetime64[ns]':
                valid_data[col] = valid_data[col].astype(dtype)

        if not quarantine.empty:
            logger.warning(f"Quarantine Alert: {len(quarantine)} malformed records identified.")
            
        return valid_data, quarantine

    @staticmethod
    def extract_rules(rules_io: StringIO) -> pd.DataFrame:
        """Ingests business rules with strict schema enforcement."""
        logger.info("Extracting business rules...")
        df = pd.read_csv(rules_io)
        
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], errors='coerce')
        
        for col, dtype in RULES_DATA_SCHEMA.items():
            if col in df.columns and dtype != 'datetime64[ns]':
                df[col] = pd.to_numeric(df[col], errors='coerce') if dtype == 'float64' else df[col].astype(dtype)
                
        return df
