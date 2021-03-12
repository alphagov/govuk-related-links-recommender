import os
from datetime import datetime, timedelta
import logging.config

from src.utils.miscellaneous import read_config_yaml, parse_sql_script
import google.auth
from google.cloud import bigquery


# Need to have active environment variable called GOOGLE_APPLICATION_CREDENTIALS pointing to json file with
# bigquery credentials


class EdgeWeightExtractor:

    def __init__(self, query_path, blocklisted_document_types, date_from=None, date_until=None):
        self.logger = logging.getLogger('make_functional_edges_and_weights.EdgeWeightExtractor')
        self.blocklisted_document_types = bloklisted_document_types

        self.date_from = date_from
        self.date_until = date_until
        self.query_path = query_path
        self.df = None
        credentials, project_id = google.auth.default()
        self.logger.info(f'creating bigqquery client for project {project_id}')
        self.client = bigquery.Client(
            credentials=credentials,
            project=project_id
        )
        self.logger.info(f'reading query from {query_path}')
        self.query_edge_list = parse_sql_script(self.query_path)

        self.query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("from_date", "STRING", self.date_from),
                bigquery.ScalarQueryParameter("to_date", "STRING", self.date_until),
                bigquery.ArrayQueryParameter("excluded_document_types", "STRING", self.blocklisted_document_types)

            ]
        )

    def create_df(self):
        self.logger.info(f'running query from {self.query_path}')
        self.df = self.client.query(self.query_edge_list, job_config=self.query_config).to_dataframe()

    def extract_df_to_csv(self, file_path):
        self.df.to_csv(file_path,
                       index=False)


if __name__ == "__main__":
    logging.config.fileConfig('src/logging.conf')
    module_logger = logging.getLogger('make_functional_edges_and_weights')
    data_dir = os.getenv("DATA_DIR")
    blocklisted_document_types = read_config_yaml(
        "document_types_excluded_from_the_topic_taxonomy.yml")['document_types']

    preprocessing_config = read_config_yaml(
        "preprocessing-config.yml")

    to_date = (datetime.today() - timedelta(preprocessing_config['to_days_ago'])).strftime('%Y%m%d')
    from_date = (datetime.today() - timedelta(preprocessing_config['from_days_ago'])).strftime('%Y%m%d')

    if preprocessing_config['use_intraday']:
        module_logger.info(f'running all user query on intraday')
        edge_weights = EdgeWeightExtractor('src/data_preprocessing/intra_day_content_id_edge_weights.sql',
                                           blocklisted_document_types)

    else:
        module_logger.info(f'running all user query between {from_date} and {to_date}')
        edge_weights = EdgeWeightExtractor('src/data_preprocessing/query_content_id_edge_weights.sql',
                                           blocklisted_document_types, from_date, to_date)

    edge_weights.create_df()


    module_logger.info(
        f'saving edges and weights to {os.path.join(data_dir, "tmp", preprocessing_config["network_filename"])}')
    edge_weights.extract_df_to_csv(os.path.join(data_dir, "tmp", preprocessing_config['network_filename']))
