import pandas as pd


class BigQueryTestDataExtractor:
    def retrieve_data_from_big_query(self):
        return pd.read_csv('tests/unit/fixtures/test_raw_bq_output.csv')