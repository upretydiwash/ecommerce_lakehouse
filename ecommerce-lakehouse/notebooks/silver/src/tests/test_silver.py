from pyspark.sql import functions as F


class TestSilver:

    @staticmethod
    def test_negative_amount(df, items):

        invalid_condition = None
        for item in items:
            condition = F.col(item) <= 0
            if invalid_condition is None:
                invalid_condition = condition
            else:
                invalid_condition = invalid_condition | condition
        
        df_failed = df.filter(invalid_condition)
        df_passed = df.filter(~invalid_condition)
        
        return df_passed, df_failed