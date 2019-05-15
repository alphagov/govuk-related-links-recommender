import os
from datetime import datetime, timedelta
from src.utils.miscellaneous import get_excluded_document_types, read_query
import google.auth
from google.cloud import bigquery

# Need to have active environment variable called GOOGLE_APPLICATION_CREDENTIALS pointing to json file with
# bigquery credentials

if __name__ == "__main__":

    data_dir = os.getenv("DATA_DIR")
    blacklisted_document_types = ",".join(get_excluded_document_types()).split(",")
    yesterday = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
    three_weeks_ago = (datetime.today() - timedelta(22)).strftime('%Y%m%d')

    credentials, project_id = google.auth.default()
    client = bigquery.Client(
        credentials=credentials,
        project=project_id
    )

    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("three_weeks_ago", "STRING", three_weeks_ago),
            bigquery.ScalarQueryParameter("yesterday", "STRING", yesterday),
            bigquery.ArrayQueryParameter("excluded_document_types", "STRING", blacklisted_document_types)
        ]
    )

    query_edge_list = read_query("content_id_edge_weights.sql")
    df = client.query(query_edge_list, job_config=query_config).to_dataframe()
    df.to_csv(os.path.join(data_dir, "tmp", "functional_edges_dict.csv.gz"), index=False)
