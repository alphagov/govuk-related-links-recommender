import os
import pandas as pd
from datetime import datetime, timedelta
from src.utils.miscellaneous import get_excluded_document_types, read_query


class BigQueryDataExtractor:
    def __init__(self):
        self.data_dir = os.getenv('DATA_DIR')
        self.bq_key = os.getenv('BIG_QUERY_KEY')
        self.project_id = os.getenv('BIG_QUERY_PROJECT')

        self.blacklisted_document_types = ", ".join(get_excluded_document_types())

        self.yesterday = (datetime.today()- timedelta(1)).strftime('%Y%m%d')
        self.three_weeks_ago = (datetime.today()- timedelta(22)).strftime('%Y%m%d')
# TODO this query gets content_ids but we may want to restrict to where Occurences>1 in the query rather than dropping later
        query = read_query('content_id_sequences.sql')
        self.query = query.format(three_weeks_ago=self.three_weeks_ago, yesterday=self.yesterday,
                                  blacklisted_document_types=self.blacklisted_document_types)

    def retrieve_data_from_big_query(self):
        # previous call for 2 weeks data cost $0.12
        return pd.read_gbq(self.query,
                           project_id=self.project_id,
                           reauth=False,
                           private_key=self.bq_key,
                           dialect="standard")


# TODO only save out columns needed downstream
# output_df.to_csv(os.path.join(DATA_DIR, "tmp",  "functional_network_data.csv"), index=False)


