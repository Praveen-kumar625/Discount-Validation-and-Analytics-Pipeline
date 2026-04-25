import sqlite3
import pandas as pd
from src.config import logger, DB_NAME

class Loader:
    """Handles persistence to SQLite with ACID compliance."""

    @staticmethod
    def load_to_sqlite(df: pd.DataFrame, table_name: str) -> None:
        """Saves a DataFrame to the SQLite database."""
        logger.info(f"Loading {len(df)} records into {table_name}...")
        
        # Prepare for SQL (format datetimes as strings)
        out_df = df.copy()
        for col in out_df.select_dtypes(include=['datetime64']).columns:
            out_df[col] = out_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Drop internal helper columns if present
        if 'potential_disc' in out_df.columns:
            out_df = out_df.drop(columns=['potential_disc'])

        with sqlite3.connect(DB_NAME) as conn:
            out_df.to_sql(table_name, conn, if_exists='append', index=False)
            
        logger.info(f"Successfully committed to {table_name}.")
