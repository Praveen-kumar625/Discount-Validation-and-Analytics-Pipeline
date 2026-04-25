# Author: khushboo kaushik
import pandas as pd
from src.config import logger

class Analytics:
    """
    This is the reporting branch. We distill all that raw data into 
    metrics that a business owner would actually care about.
    """

    @staticmethod
    def generate_impact_report(df_final: pd.DataFrame, df_quarantine: pd.DataFrame) -> None:
        """
        Crunch the numbers and print a nice summary report to the console.
        We're tracking revenue, burn, and what we successfully blocked.
        """
        logger.info("Crunching the numbers for the Impact Report...")
        
        accepted = df_final[df_final['is_accepted']]
        
        # What we actually made vs what we would've made without any discounts
        actual_rev = accepted.groupby('order_id')['final_payable_amount'].first().sum()
        base_rev = df_final.groupby('order_id')['original_price'].first().sum()
        
        # Revenue Protected: the dollar value of the discounts we rejected (expired, conflicts, etc.)
        blocked_value = df_final[~df_final['is_accepted'] & df_final['discount_type'].notna()]['potential_disc'].sum()

        print("\n" + "═"*60)
        print("          DISCOUNT VALIDATOR: BUSINESS IMPACT REPORT")
        print("═"*60)
        print(f"PIPELINE VOLUME:")
        print(f"  Processed: {len(df_final)} | Accepted: {len(accepted)} | Rejected: {(~df_final['is_accepted']).sum()}")
        print(f"  Quarantined (DLQ): {len(df_quarantine)}")
        
        print(f"\nFINANCIAL METRICS:")
        print(f"  Actual Revenue:         ${actual_rev:,.2f}")
        print(f"  Counterfactual Revenue: ${base_rev:,.2f}")
        print(f"  Total Discount Burn:    ${(base_rev - actual_rev):,.2f}")
        
        print(f"\nLOSS PREVENTION:")
        print(f"  Revenue Protected:      ${blocked_value:,.2f}")
        
        print(f"\nTOP REJECTION REASONS:")
        rejections = df_final['rejection_reason'].value_counts()
        if not rejections.empty:
            print(rejections.head(3).to_string())
        else:
            print("  None (It's a miracle, everyone's coupons worked!)")
        print("═"*60 + "\n")
