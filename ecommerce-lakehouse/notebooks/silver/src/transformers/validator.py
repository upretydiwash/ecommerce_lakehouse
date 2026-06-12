from pyspark.sql import functions as F

class Validator:

    @staticmethod
    def validate_required_columns(df, required_columns):
        # Filter out columns that don't exist in the DataFrame
        valid_columns = [column for column in required_columns if column in df.columns]
        print(f"Validating required columns: {valid_columns}")
        select_exprs = [F.col(c) for c in df.columns]  # Keep all existing columns
        # Only process valid columns
        for column in valid_columns:
            select_expr.app
            df = df.filter(col(column).isNotNull())
        
        return df