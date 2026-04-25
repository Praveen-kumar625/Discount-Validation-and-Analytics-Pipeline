import pandas as pd
import numpy as np
from src.config import logger

class Transformer:
    """Core Rules Engine and Business Logic."""

    @staticmethod
    def apply_rules_engine(df_applied: pd.DataFrame, df_rules: pd.DataFrame) -> pd.DataFrame:
        """Executes vectorized validation rules and resolves exclusivity conflicts."""
        logger.info("Executing Vectorized Rules Engine...")
        
        # 1. Join Datasets
        df = df_applied.merge(df_rules, on='coupon_code', how='left')
        df['rejection_reason'] = None
        
        # 2. Expiry Validation
        df.loc[df['order_date'] > df['expiry_date'], 'rejection_reason'] = 'ERR_EXPIRED'
        
        # 3. Scope Validation
        scope_mask = ~((df['applicable_scope'] == df['product_category']) | (df['applicable_scope'] == df['product_id']))
        df.loc[scope_mask & df['rejection_reason'].isna(), 'rejection_reason'] = 'ERR_CAT_MISMATCH'
        
        # 4. Deduplication
        dupe_mask = df.duplicated(subset=['order_id', 'coupon_code'], keep='first')
        df.loc[dupe_mask & df['rejection_reason'].isna(), 'rejection_reason'] = 'ERR_DUPE'
        
        # 5. Potential Discount Calculation (for conflict resolution)
        df['potential_disc'] = 0.0
        valid_mask = df['rejection_reason'].isna()
        
        is_pct = df['discount_type'] == 'percentage'
        is_fix = df['discount_type'] == 'fixed'
        
        df.loc[valid_mask & is_pct, 'potential_disc'] = np.minimum(
            df['original_price'] * (df['discount_value'] / 100.0), 
            df['max_cap'].fillna(np.inf)
        )
        df.loc[valid_mask & is_fix, 'potential_disc'] = df['discount_value']
        
        # 6. Exclusivity Conflict Resolution (O(N log N))
        # Logic: If multiple valid exclusive coupons exist, pick the one with max discount
        df = df.sort_values(['order_id', 'potential_disc'], ascending=[True, False])
        
        # Find conflicts
        df_valid = df[df['rejection_reason'].isna()].copy()
        conflict_mask = df_valid.groupby('order_id').cumcount() > 0
        df.loc[df_valid[conflict_mask].index, 'rejection_reason'] = 'ERR_CONFLICT'
        
        return df

    @staticmethod
    def calculate_financials(df: pd.DataFrame) -> pd.DataFrame:
        """Finalizes discount amounts and payable totals with floor constraints."""
        logger.info("Finalizing financial calculations...")
        df['is_accepted'] = df['rejection_reason'].isna()
        
        # Zero out discounts for rejected records
        df['discount_amount'] = np.where(df['is_accepted'], df['potential_disc'], 0.0)
        
        # Net calculation with price floor (cannot be negative)
        df['final_payable_amount'] = (df['original_price'] - df['discount_amount']).clip(lower=0.0)
        
        return df
