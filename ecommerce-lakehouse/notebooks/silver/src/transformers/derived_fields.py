from pyspark.sql import functions as F


class DerivedFields:

    @staticmethod
    def extract_dates_time(df, columns):
        """
        Extract date/time components from timestamp columns.
    
        Args:
            df: Input DataFrame
            columns: List of timestamp column names to extract from
        
        Returns:
            DataFrame with additional date/time component columns
        """
        # Build all transformations in a single select
        select_exprs = [F.col(c) for c in df.columns]  # Keep all existing columns
    
        for column_name in columns:
            col_ref = F.col(column_name)
            select_exprs.extend([
                F.year(col_ref).alias(f"{column_name}_year"),
                F.month(col_ref).alias(f"{column_name}_month"),
                F.dayofmonth(col_ref).alias(f"{column_name}_day"),
                F.hour(col_ref).alias(f"{column_name}_hour"),
                F.minute(col_ref).alias(f"{column_name}_minute"),
                F.second(col_ref).alias(f"{column_name}_second")
            ])
    
        return df.select(*select_exprs)
