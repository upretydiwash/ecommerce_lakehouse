from pyspark.sql.types import IntegerType, DoubleType, FloatType, BooleanType, DateType, TimestampType
from pyspark.sql import functions as F

class DataTypeConverter:
    """
    Utility class for efficient data type conversions in Spark DataFrames.
    All conversions are performed in a single transformation for optimal performance.
    """
    
    TYPE_MAP = {
        'int': IntegerType(),
        'integer': IntegerType(),
        'double': DoubleType(),
        'float': FloatType(),
        'boolean': BooleanType(),
        'bool': BooleanType(),
        'date': DateType(),
        'timestamp': TimestampType()
    }
    
    @staticmethod
    def convert_columns(df, column_type_map):
        """
        Convert multiple columns to specified types in a single transformation.
        
        Args:
            df: Input DataFrame
            column_type_map: Dictionary mapping column names to target types
                           e.g., {'col1': 'int', 'col2': 'double'}
                           or {'col1': IntegerType(), 'col2': DoubleType()}
        
        Returns:
            DataFrame with converted column types
            
        Example:
            df = DataTypeConverter.convert_columns(df, {
                'age': 'int',
                'salary': 'double',
                'is_active': 'boolean'
            })
        """
        select_exprs = []
        converted_cols = set()
        
        for column_name in df.columns:
            if column_name in column_type_map:
                target_type = column_type_map[column_name]
                
                # Handle string type names
                if isinstance(target_type, str):
                    target_type = DataTypeConverter.TYPE_MAP.get(target_type.lower())
                    if target_type is None:
                        raise ValueError(f"Unknown type: {column_type_map[column_name]}")
                
                select_exprs.append(F.col(column_name).cast(target_type).alias(column_name))
                converted_cols.add(column_name)
            else:
                select_exprs.append(F.col(column_name))
        
        return df.select(*select_exprs)
    
    # Legacy methods for backward compatibility (now optimized)
    @staticmethod
    def convertToInt(df, columns):
        """Convert columns to IntegerType in a single transformation."""
        return DataTypeConverter._convert_to_type(df, columns, IntegerType())
    
    @staticmethod
    def convertToDouble(df, columns):
        """Convert columns to DoubleType in a single transformation."""
        return DataTypeConverter._convert_to_type(df, columns, DoubleType())
    
    @staticmethod
    def convertToFloat(df, columns):
        """Convert columns to FloatType in a single transformation."""
        return DataTypeConverter._convert_to_type(df, columns, FloatType())
    
    @staticmethod
    def convertToBoolean(df, columns):
        """Convert columns to BooleanType in a single transformation."""
        return DataTypeConverter._convert_to_type(df, columns, BooleanType())
    
    @staticmethod
    def convertToDate(df, columns):
        """Convert columns to DateType in a single transformation."""
        return DataTypeConverter._convert_to_type(df, columns, DateType())
    
    @staticmethod
    def convertToTimestamp(df, columns):
        """Convert columns to TimestampType in a single transformation."""
        return DataTypeConverter._convert_to_type(df, columns, TimestampType())
    
    @staticmethod
    def _convert_to_type(df, columns, target_type):
        """Internal helper to convert multiple columns to a single type efficiently."""
        select_exprs = []
        
        for col_name in df.columns:
            if col_name in columns:
                select_exprs.append(F.col(col_name).cast(target_type).alias(col_name))
            else:
                select_exprs.append(F.col(col_name))
        
        return df.select(*select_exprs)
