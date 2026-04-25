import pandas as pd
from src.config import logger

class Analytics:
    """Generates business intelligence insights from pipeline results."""

    @staticmethod
    def generate_impact_report(df_final: pd.DataFrame, df_quarantine: pd.DataFrame) -> None:
        """Prints a structured business impact summary."""
        logger.info("Generating Business Impact Report...")
        
        accepted = df_final[df_final['is_accepted']]
        
        # Financials
        actual_rev = accepted.groupby('order_id')['final_payable_amount'].first().sum()
        base_rev = df_final.groupby('order_id')['original_price'].first().sum()
        
        # Revenue Protected (blocked discounts)
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
            print("  None (All coupons accepted)")
        print("═"*60 + "\n")
