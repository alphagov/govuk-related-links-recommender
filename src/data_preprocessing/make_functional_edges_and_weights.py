import os
from datetime import datetime, timedelta
import logging.config

from src.utils.miscellaneous import read_exclusions_yaml, read_file_as_string, safe_getenv
import google.auth
from google.cloud import bigquery


logging.config.fileConfig('src/logging.conf')


# Need to have active environment variable called GOOGLE_APPLICATION_CREDENTIALS pointing to json file with
# bigquery credentials


class EdgeWeightExtractor:
    def __init__(self, blocklisted_document_types, date_from, date_until):
        self.blocklisted_document_types = blocklisted_document_types
        self.date_from = date_from
        self.date_until = date_until

        logger = logging.getLogger('make_functional_edges_and_weights.EdgeWeightExtractor')
        credentials, project_id = google.auth.default()
        logger.info(f'creating bigqquery client for project {project_id}')
        self.client = bigquery.Client(
            credentials=credentials,
            project=project_id
        )
        logger.info('reading query from  src/data_preprocessing/query_content_id_edge_weights.sql')
        self.query_edge_list = read_file_as_string("src/data_preprocessing/query_content_id_edge_weights.sql")

        self.query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("three_weeks_ago", "STRING", self.date_from),
                bigquery.ScalarQueryParameter("yesterday", "STRING", self.date_until),
                bigquery.ArrayQueryParameter("excluded_document_types", "STRING", self.blocklisted_document_types)
            ]
        )

        self.df = self.client.query(self.query_edge_list, job_config=self.query_config).to_dataframe()

    def extract_df_to_csv(self, file_path):
        self.df.to_csv(file_path,
                       index=False)


if __name__ == "__main__":
    data_dir = safe_getenv('DATA_DIR')
    functional_edges_output_filename = os.path.join(data_dir, 'functional_edges.csv')

    module_logger = logging.getLogger('make_functional_edges_and_weights')
    blocklisted_document_types = read_exclusions_yaml(
        "document_types_excluded_from_the_topic_taxonomy.yml")['document_types']
    two_days_ago = (datetime.today() - timedelta(2)).strftime('%Y%m%d')
    start_date = (datetime.today() - timedelta(24)).strftime('%Y%m%d')

    module_logger.info(f'running query between {two_days_ago} and {start_date}')
    edge_weights = EdgeWeightExtractor(blocklisted_document_types, start_date, two_days_ago)

    module_logger.info(f'saving edges and weights to {functional_edges_output_filename}')
    edge_weights.extract_df_to_csv(functional_edges_output_filename)
