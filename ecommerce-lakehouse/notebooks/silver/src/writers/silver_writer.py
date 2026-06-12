class SilverWriter:
    @staticmethod
    def write_to_table(df, table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

