from pyspark.sql.functions import col, initcap

class DataCleanser:

    #remove nulls
    @staticmethod    
    def drop_nulls(df, sub):
        df = df.dropna(subset=sub)
        return df
    @staticmethod
    def capitalized(df, capitalize_columns):
        select_expr= []
        for column in df.columns:
            if column in capitalize_columns:
                select_expr.append(initcap(col(column)).alias(column))
            else:
                select_expr.append(col(column))
        df = df.select(*select_expr)
        return df
    #drop duplicates
    @staticmethod
    def drop_duplicates(df):
        df = df.dropDuplicates()
        return df
        