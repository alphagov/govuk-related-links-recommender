import os
from datetime import datetime, timedelta
import logging.config

from src.utils.miscellaneous import read_exclusions_yaml, read_query
import google.auth
from google.cloud import bigquery


logging.config.fileConfig('src/logging.conf')


# Need to have active environment variable called GOOGLE_APPLICATION_CREDENTIALS pointing to json file with
# bigquery credentials


class EdgeWeightExtractor:
    logger = logging.getLogger('make_functional_edges_and_weights.EdgeWeightExtractor')
    credentials, project_id = google.auth.default()
    logger.info(f'creating bigqquery client for project {project_id}')
    client = bigquery.Client(
        credentials=credentials,
        project=project_id
    )
    logger.info('reading query from  src/data_preprocessing/query_content_id_edge_weights.sql')
    query_edge_list = read_query("src/data_preprocessing/query_content_id_edge_weights.sql")

    def __init__(self, blacklisted_document_types, date_from, date_until):
        self.blacklisted_document_types = blacklisted_document_types
        self.date_from = date_from
        self.date_until = date_until

        self.query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("three_weeks_ago", "STRING", self.date_from),
                bigquery.ScalarQueryParameter("yesterday", "STRING", self.date_until),
                bigquery.ArrayQueryParameter("excluded_document_types", "STRING", self.blacklisted_document_types)
            ]
        )

        self.df = self.client.query(self.query_edge_list, job_config=self.query_config).to_dataframe()

    def extract_df_to_csv(self, file_path):
        self.df.to_csv(file_path,
                       index=False)


if __name__ == "__main__":
    module_logger = logging.getLogger('make_functional_edges_and_weights')
    DATA_DIR = os.getenv("DATA_DIR")
    blacklisted_document_types = read_exclusions_yaml(
    "document_types_excluded_from_the_topic_taxonomy.yml")['document_types']
    yesterday = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
    three_weeks_ago = (datetime.today() - timedelta(22)).strftime('%Y%m%d')

    module_logger.info(f'running query between {yesterday} and {three_weeks_ago}')
    edge_weights = EdgeWeightExtractor(blacklisted_document_types, three_weeks_ago, yesterday)

    module_logger.info(f'saving edges and weights to {os.path.join(data_dir, "tmp", "functional_edges.csv")}')
    edge_weights.extract_df_to_csv(os.path.join(DATA_DIR, "tmp", "functional_edges.csv"))
