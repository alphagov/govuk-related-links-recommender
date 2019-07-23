from src.utils.miscellaneous import read_file_as_string

import google.auth
from google.cloud import bigquery
import logging.config

logging.config.fileConfig('src/logging.conf')


class BigQueryClient:
    def __init__(self, project_id=None, credentials=None):
        if credentials is None:
            credentials, project_id = google.auth.default()
        self.client = bigquery.Client(credentials=credentials, project=project_id)
        self.logger = logging.getLogger('big_query_client')

    def query_page_views(self, start_date, end_date):
        self.logger.info('Querying BigQuery for content_ids')

        query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
                bigquery.ScalarQueryParameter("end_date", "STRING", end_date)
            ]
        )
        query_string = read_file_as_string("src/models/query_eligible_source_content_ids.sql")

        df = self.client.query(query_string, job_config=query_config).to_dataframe()
        self.logger.info(f'Got page views for {df.shape[0]} content_ids.')

        return df
