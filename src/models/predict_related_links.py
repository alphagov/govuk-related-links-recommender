# Use our saved down node2vec model to get recs for each content ID
# Make sure to add exclusions - smart answers, simple smart answers, fatality notices, etc. + a file in the repo with a list of hardcoded specific exclusions
# Use document types from content store data?
# Does it break if there aren't 1000 recs for each link? maybe add a try around that to catch any exceptions
from builtins import staticmethod

from gensim.models import Word2Vec
from datetime import datetime, timedelta
import pandas as pd
import os
import json

import google.auth
from google.cloud import bigquery
from src.utils.miscellaneous import read_query, load_pickled_content_id_list
from tqdm import tqdm

import logging.config
logging.config.fileConfig('src/logging.conf')

DATA_DIR = os.getenv("DATA_DIR")

# TODO check probability threshold is correct 0.46
# TODO check maximum 5 related links is correct


def is_target_content_id_eligible(content_id, eligible_target_links):
    """
    Boolean: checks presence of the content_id in the known eligible_target_links
    :param content_id: string content_id
    :param eligible_target_links: list of content_ids
    :return: boolean True id content_id should be included
    """
    return content_id in eligible_target_links


def only_include_eligible_target_content_ids(df_target_prop, eligible_target_links):
    """
    Selects the rows of a pandas DataFrame which contain eligible target_content_ids. Drops rows where target_content_id
    is not in the list of eligible target content_ids
    :param df_target_prop: pandas DataFrame which includes column named 'target_content_id'
    :param eligible_target_links: list of eligible content_ids to identify rows where the target_content_id should be included.
    :return: pandas DataFrame of eligible target_content_ids
    """
    return df_target_prop.query('target_content_id in @eligible_target_links')


def get_related_links_for_a_source_content_id(source_content_id, model, eligible_target_content_ids,
                                              probability_threshold=0.46, output_type="list"):
    """
    Gets the top-5 most-probable eligible target_content_ids for a single source_content_id.
    Target_content_ids are dropped if:
        - The predicted probability between source and target is below the probability threshold
        - The target_content_id is not listed in the inclusion list
        - The source and target are the same item
        - The link is not in the top 5 (highest probabilities) for that source_id
    :param source_content_id: string content_id
    :param model: node2vec model where model.wv.vocab.keys() are content_ids
    :param eligible_target_content_ids: list of content_ids we can link to
    :param probability_threshold: the models predicted probability of a link between source_content_id and
    target_content_id
    :return: pandas DataFrame with a maximum of 5 rows where each row contains a 'source_content_id',
    'target_content_id' and 'probability'
    """
    potential_related_links = pd.DataFrame(model.wv.most_similar(source_content_id, topn=1000))
    potential_related_links.columns = ['target_content_id', 'probability']
    potential_related_links.sort_values('probability', inplace=True, ascending=False)
    potential_related_links = only_include_eligible_target_content_ids(potential_related_links,
                                                                       eligible_target_content_ids,
                                                                    )
    potential_related_links['source_content_id'] = source_content_id

    if output_type == "df":
        output = potential_related_links[potential_related_links['probability'] > probability_threshold].head(5)
    if output_type == "list":
        output = potential_related_links[potential_related_links['probability'] > probability_threshold].head(5)[
            'target_content_id'].values.tolist()

    return output


class RelatedLinksJson:
    """
    Uses a node2vec model to create a nested list of source_content_ids and their predicted target_content_ids (up to 5)
    """
    def __init__(self,
                 eligible_source_content_ids,
                 eligible_target_content_ids,
                 model):
        self.model = model
        self.eligible_target_content_ids = eligible_target_content_ids
        logging.info("Getting eligible source content_ids")
        self.eligible_source_content_ids = [
            content_id for content_id in tqdm(
                eligible_source_content_ids, desc="eligible_content_ids"
            ) if content_id in self.model.wv.vocab.keys()
        ]
        logging.info("retrieving and processing target_content_ids for each source_content_id")
        self.related_links = [
            get_related_links_for_a_source_content_id(content_id, self.model, self.eligible_target_content_ids)
            for
            content_id in
            tqdm(self.eligible_source_content_ids, desc="getting related links")]

    def export_related_links_to_json(self, file_path):
        """
        Converts a nested list of source_content_ids, each with up to 5 target_content_ids to a python dict and exports
        to a json file
        :param file_path:
        :return:
        """
        with open(file_path, 'w') as f:
            json.dump(
                dict(zip(self.eligible_source_content_ids, self.related_links)), f, ensure_ascii=False)


class RelatedLinksCsv:
    with open(
            os.path.join(DATA_DIR, 'tmp', 'content_id_base_path_mapping.json'),
            'r') as content_id_to_base_path_mapping_file:
        content_id_to_base_path_mapper = json.load(
            content_id_to_base_path_mapping_file)

    def __init__(self, top100_content_ids, eligible_target_content_ids, model):

        self.top100_content_ids_df = top100_content_ids[top100_content_ids['source_content_id'].isin(model.wv.vocab.keys())]
        logging.info("extracting related links using apply on the source_content_id column ")
        self.top100_related_links_series = self._get_related_links_for_df(self.top100_content_ids_df,
                                                                          model,
                                                                          eligible_target_content_ids)
        logging.info("combining series of dfs into single df")
        self.top100_related_links_df = self._series_to_df(self.top100_related_links_series)
        logging.info("adding base_path columns using dict mapping from content_ids to base_path")
        self.top100_related_links_df = self._get_base_paths(self.top100_related_links_df)

    @staticmethod
    def _get_related_links_for_df(df, model, eligible_target_content_ids):
        """
        most probable (5 max) and eligible target_content_ids are extracted for each source_content_id
        in the input df's column called 'content_id'
        :param df: pandas DataFrame with a column named 'content_id'
        :param model: node2vec model where mode.wv.vocab.keys() are content_ids
        :param eligible_target_content_ids: list of eligible target_content_ids
        :return: pandas Series where each element is a pandas DataFrame
        """
        return df['source_content_id'].apply(
            get_related_links_for_a_source_content_id,
            model=model,
            eligible_target_content_ids=eligible_target_content_ids,
            output_type="df")

    @staticmethod
    def _series_to_df(series):
        """
        Concatenates each pandas DataFrame in a series of dfs into a single df
        :param series: pandas Series where each item is a DataFrame
        :return: pandas DataFrame
        """
        df = pd.DataFrame()
        for related_links_for_one_content_id in series:
            df = pd.concat([df, related_links_for_one_content_id])
        return df

    def _get_base_paths(self, df):
        """
        Add two columns to pandas DataFrame giving the base_paths for the source and target content_ids
        :param df: pandas DataFrame with columns named 'source_content_id' and 'target_content_id'
        :return:
        """
        df['source_base_path'] = df['source_content_id'].map(
            self.content_id_to_base_path_mapper)

        df['target_base_path'] = df['target_content_id'].map(
            self.content_id_to_base_path_mapper)
        return df

    def write_to_csv(self, file_path):
        """
        Exports DataFrame containing links to csv
        :param file_path:
        :return:
        """
        self.top100_related_links_df.to_csv(file_path, index=False)


if __name__ == '__main__':
    module_logger = logging.getLogger('predict_related_links')

    module_logger.info(
        f'loading model from "models/n2v.model"')
    trained_model = Word2Vec.load("models/n2v.model")

    module_logger.info(
        f'loading eligible_source_content_ids from {DATA_DIR}/tmp/eligible_source_content_ids.pkl')
    eligible_source_content_ids = load_pickled_content_id_list(os.path.join(DATA_DIR, "tmp",
                                                                            "eligible_source_content_ids.pkl"))

    eligible_target_content_ids = load_pickled_content_id_list(os.path.join(DATA_DIR, "tmp",
                                                                               "eligible_target_content_ids.pkl"))

    module_logger.info(f'creating RelatedLinksJson for {len(eligible_source_content_ids)} eligible_source_content_ids')
    related_links = RelatedLinksJson(eligible_source_content_ids,
                                     eligible_target_content_ids,
                                     trained_model)

    module_logger.info('Exporting related links')
    related_links.export_related_links_to_json(
        os.path.join(DATA_DIR, "predictions", datetime.today().strftime('%Y%m%d') + "suggested_related_links.json"))

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
            bigquery.ScalarQueryParameter("yesterday", "STRING", yesterday)
        ]
    )

    query_top_100 = read_query("src/models/query_top_100_eligible_source_content_ids.sql")
    module_logger.info('Querying BigQuery for content_ids')
    all_df = client.query(query_top_100, job_config=query_config).to_dataframe()

    all_df.to_csv(os.path.join(DATA_DIR, 'tmp', 'all_df.csv'), index=False)

    module_logger.info('Filtering to content_ids in the vocabulary')
    all_df.query('content_id in @trained_model.wv.vocab.keys() and content_id in @eligible_source_content_ids',
                 inplace=True)
    module_logger.info('Sorting by page_hits descending')
    all_df.sort_values(
        by=['page_hits'], inplace=True, ascending=False)

    all_df.rename(
        columns={
            'content_id': 'source_content_id'},
        inplace=True)

    module_logger.info('Getting top 100 content_ids')
    top100_df = all_df.head(100)

    top100_df.to_csv(os.path.join(DATA_DIR, 'tmp', 'top100.csv'), index=False)

    module_logger.info('creating RelatedLinksCsv')
    related_links_csv_writer = RelatedLinksCsv(top100_df, eligible_target_content_ids,
                                               trained_model)

    module_logger.info('Writing out related links for top 100 content_ids to csv')
    related_links_csv_writer.write_to_csv(os.path.join(DATA_DIR, "predictions",
                                                       datetime.today().strftime(
                                                           '%Y%m%d') + "top100_suggested_related_links.csv"))


# Code for how links were generated for the A/B test below, saved for the future,
# it was easier/quicker to write it in a more DataFrame-y method for now but we could revisit this later

# def compute_top_n(base_path_pandas_series, n):
#     pages_links = []
#     missing = []
#     for page in base_path_pandas_series.values:
#         # get integer id for base_path node
#         if page in url_nid.keys():
#             target = str(url_nid[page])
#             count = 0
#             cids = []
#             # use integer id to locate vecotr and find the 1000 most similar
#             vecs = generate_vectors(model.wv.most_similar(target, topn=1000))
#             # Only get 10 destination links per source
#             while count <= n:
#                 # return integer node id of the destination_node and probability
#                 nid, prob = next(vecs)
#                 # check exclusions
#                 # if the destination_content_id is ne to source_content_id
#                 if nid_cid[int(target)] != nid_cid[int(nid)] \
#                         # and topic or browse is not in the url of the source
#                     and all(t not in nid_url[int(nid)] for t in ["/topic", "/browse"]) \
#                         # and source_content_id is not already in cids (source_content_id list)
#                     and nid_cid[int(nid)] not in cids \
#                         # and the source_content_id is not an embedded links on the page
#                     and nid_cid[int(nid)] not in cid_link_cids[nid_cid[int(target)]]:
#                     # then add the source_content_id to source_content_id list
#                 cids.append(nid_cid[int(nid)])
#                 # get some attributes about the destination page
#                 page_link = {"nid": int(target),
#                              "cid": nid_cid[int(target)],
#                              "base_path": page,
#                              "link": nid_url[int(nid)],
#                              "link_cid": nid_cid[int(nid)],
#                              "probability": round(prob, 3)}
#                 # add these attributes to the destination_content_id list
#                 pages_links.append(page_link)
#                 count += 1
#
#     else:
#         missing.append(page)


# #             print("Page {} is missing from training set".format(page))
#
# return pd.DataFrame(pages_links), missing
