# Author: khushboo kaushik
import pandas as pd
import numpy as np
from src.config import logger

class Transformer:
    """
    This is where the magic happens. We take the valid coupons, 
    run them against our rulebook, resolve any exclusivity fights, 
    and calculate the final damage (financials).
    """

    @staticmethod
    def apply_rules_engine(df_applied: pd.DataFrame, df_rules: pd.DataFrame) -> pd.DataFrame:
        """
        Runs our vectorized rule set. We're checking for expiry, category 
        mismatches, and sneaky duplicate applications.
        """
        logger.info("Cranking up the Rules Engine...")
        
        # Merge the coupons with their rules so we can compare row-by-row efficiently
        df = df_applied.merge(df_rules, on='coupon_code', how='left')
        df['rejection_reason'] = None
        
        # 1. Expiry: If the order date is past the coupon's life, it's a no-go.
        df.loc[df['order_date'] > df['expiry_date'], 'rejection_reason'] = 'ERR_EXPIRED'
        
        # 2. Scope: Does this coupon even apply to this product or category?
        scope_mask = ~((df['applicable_scope'] == df['product_category']) | (df['applicable_scope'] == df['product_id']))
        df.loc[scope_mask & df['rejection_reason'].isna(), 'rejection_reason'] = 'ERR_CAT_MISMATCH'
        
        # 3. Dupes: Don't let them apply the same coupon code twice for one order.
        dupe_mask = df.duplicated(subset=['order_id', 'coupon_code'], keep='first')
        df.loc[dupe_mask & df['rejection_reason'].isna(), 'rejection_reason'] = 'ERR_DUPE'
        
        # 4. Tie-breaking: Calculate potential discount so we can pick a winner for exclusivity conflicts.
        df['potential_disc'] = 0.0
        valid_mask = df['rejection_reason'].isna()
        
        is_pct = df['discount_type'] == 'percentage'
        is_fix = df['discount_type'] == 'fixed'
        
        # Vectorized math for the potential savings
        df.loc[valid_mask & is_pct, 'potential_disc'] = np.minimum(
            df['original_price'] * (df['discount_value'] / 100.0), 
            df['max_cap'].fillna(np.inf)
        )
        df.loc[valid_mask & is_fix, 'potential_disc'] = df['discount_value']
        
        # 5. Exclusivity Resolution: If there's a conflict, the coupon with the best value wins.
        # We sort by order and discount value, then reject anything that isn't the top pick.
        df = df.sort_values(['order_id', 'potential_disc'], ascending=[True, False])
        
        df_valid = df[df['rejection_reason'].isna()].copy()
        conflict_mask = df_valid.groupby('order_id').cumcount() > 0
        df.loc[df_valid[conflict_mask].index, 'rejection_reason'] = 'ERR_CONFLICT'
        
        return df

    @staticmethod
    def calculate_financials(df: pd.DataFrame) -> pd.DataFrame:
        """
        Finalizes the numbers. If a coupon was rejected, its discount is $0.
        We also make sure we don't accidentally end up with a negative price.
        """
        logger.info("Finalizing the financial math...")
        df['is_accepted'] = df['rejection_reason'].isna()
        
        # Only apply discounts for coupons that survived the rules engine
        df['discount_amount'] = np.where(df['is_accepted'], df['potential_disc'], 0.0)
        
        # Clip at zero just in case a coupon is worth more than the product
        df['final_payable_amount'] = (df['original_price'] - df['discount_amount']).clip(lower=0.0)
        
        return df
