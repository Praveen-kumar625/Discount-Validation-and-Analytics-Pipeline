# Author: khushboo kaushik
import sqlite3
import pandas as pd
from src.config import logger, DB_NAME

class Loader:
    """
    Persistence layer. We take our processed DataFrames and dump 
    them into SQLite so we have a permanent audit trail.
    """

    @staticmethod
    def load_to_sqlite(df: pd.DataFrame, table_name: str) -> None:
        """
        Saves the results. We handle datetime formatting and 
        drop any internal helper columns before committing.
        """
        logger.info(f"Shipping {len(df)} records to the '{table_name}' table...")
        
        # SQL doesn't always play nice with native Pandas datetimes, so we stringify them
        out_df = df.copy()
        for col in out_df.select_dtypes(include=['datetime64']).columns:
            out_df[col] = out_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # 'potential_disc' is just an internal engine calculation, we don't need to store it.
        if 'potential_disc' in out_df.columns:
            out_df = out_df.drop(columns=['potential_disc'])

        # Context manager ensures we close the connection and commit the transaction properly
        with sqlite3.connect(DB_NAME) as conn:
            out_df.to_sql(table_name, conn, if_exists='append', index=False)
            
        logger.info(f"Done! Data is safely stored in '{table_name}'.")
