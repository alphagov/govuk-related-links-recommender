# Use our saved down node2vec model to get recs for each content ID
# Make sure to add exclusions - smart answers, simple smart answers, fatality notices, etc. + a file in the repo with a list of hardcoded specific exclusions
# Use document types from content store data?
# Does it break if there aren't 1000 recs for each link? maybe add a try around that to catch any exceptions
from builtins import staticmethod

from gensim.models import Word2Vec
from datetime import datetime, timedelta
import pandas as pd
import os
import pickle
import json

import google.auth
from google.cloud import bigquery
from src.utils.miscellaneous import get_excluded_document_types, read_query
from tqdm import tqdm

DATA_DIR = os.getenv("DATA_DIR")


# TODO check probability threshold is correct 0.46
# TODO check maximum 5 related links is correct


def load_pickled_content_id_list(filename):
    with open(os.path.join(DATA_DIR, "tmp", filename),
              "rb") as input_file:
        id_list = pickle.load(input_file)
    return ", ".join(id_list)


def is_target_content_id_eligible(content_id, excluded_target_links):
    return content_id not in excluded_target_links


def exclude_ineligible_target_content_ids(df_target_prop, excluded_target_links, source_content_id):
    df = df_target_prop.loc[
        df_target_prop['target_content_id'].apply(is_target_content_id_eligible,
                                                  excluded_target_links=excluded_target_links)]
    return df.loc[df.target_content_id != source_content_id]


def get_related_links_for_a_source_content_id_in_df(source_content_id, model, excluded_target_content_ids,
                                                    probability_threshold=0.46):
    potential_related_links = pd.DataFrame(model.wv.most_similar(source_content_id, topn=1000))
    potential_related_links.columns = ['target_content_id', 'probability']
    potential_related_links.sort_values('probability', inplace=True, ascending=False)
    potential_related_links = exclude_ineligible_target_content_ids(potential_related_links, excluded_target_content_ids,
                                                                    source_content_id)
    potential_related_links['source_content_id'] = source_content_id

    return potential_related_links[potential_related_links['probability'] > probability_threshold].head(5)


def get_related_links_for_a_source_content_id_in_list(source_content_id, model, excluded_target_content_ids,
                                                      probability_threshold=0.46):
    potential_related_links = pd.DataFrame(model.wv.most_similar(source_content_id, topn=1000))
    potential_related_links.columns = ['target_content_id', 'probability']
    potential_related_links.sort_values('probability', inplace=True, ascending=False)
    potential_related_links = exclude_ineligible_target_content_ids(potential_related_links, excluded_target_content_ids,
                                                                    source_content_id)

    return potential_related_links[potential_related_links['probability'] > probability_threshold].head(5)[
        'target_content_id'].values.tolist()


class RelatedLinksJson:
    def __init__(self,
                 eligible_source_content_ids,
                 excluded_target_content_ids,
                 model=Word2Vec.load("models/n2v.model")):
        self.model = model
        self.excluded_target_content_ids = excluded_target_content_ids
        self.eligible_source_content_ids = [
            content_id for content_id in tqdm(eligible_source_content_ids, desc="eligible_content_ids") if content_id in
                                                                        self.model.wv.vocab.keys()]
        self.related_links = [
            get_related_links_for_a_source_content_id_in_list(content_id, self.model, self.excluded_target_content_ids)
            for
            content_id in
            tqdm(self.eligible_source_content_ids, desc="getting related links")]

    def export_related_links_to_json(self, file_path):
        with open(file_path, 'w') as f:
            json.dump(
                dict(zip(self.eligible_source_content_ids, self.related_links)), f, ensure_ascii=False)


class RelatedLinksCsv:
    with open(
            os.path.join(DATA_DIR, 'tmp', 'content_id_base_path_mapping.json'),
            'r') as content_id_to_base_path_mapping_file:
        content_id_to_base_path_mapper = json.load(
            content_id_to_base_path_mapping_file)

    def __init__(self, top100, excluded_target_content_ids, model):
        self.top100 = top100[top100['content_id'].isin(model.wv.vocab.keys())]
        self.top100_related_links_series = self.__get_related_links_for_df(top100,
                                                                           model,
                                                                           excluded_target_content_ids)

        self.top100_related_links_df = self.__series_to_df(self.top100_related_links_series)

        self.top100_related_links_df = self.__get_base_paths(self.top100_related_links_df)

    @staticmethod
    def __get_related_links_for_df(df, model, excluded_target_content_ids):
        return df['content_id'].apply(
            get_related_links_for_a_source_content_id_in_df,
            model=model,
            excluded_target_content_ids=excluded_target_content_ids)

    @staticmethod
    def __series_to_df(series):
        df = pd.DataFrame()
        for related_links_for_one_content_id in series:
            df = pd.concat([df, related_links_for_one_content_id])
        return df

    def __get_base_paths(self, df):
        df['source_base_path'] = df['source_content_id'].map(
            self.content_id_to_base_path_mapper)

        df['target_base_path'] = df['target_content_id'].map(
            self.content_id_to_base_path_mapper)
        return df

    def write_to_csv(self, file_path):
        self.top100_related_links_df.to_csv(file_path, index=False)


if __name__ == '__main__':
    eligible_source_content_ids = load_pickled_content_id_list("eligible_source_content_ids.pkl")
    related_links = RelatedLinksJson(eligible_source_content_ids,
                                     load_pickled_content_id_list("excluded_target_content_ids.pkl"))

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
            bigquery.ScalarQueryParameter("yesterday", "STRING", yesterday),
            bigquery.ArrayQueryParameter("eligible_source_content_ids", "STRING", eligible_source_content_ids)
        ]
    )

    query_top_100 = read_query("query_top_100_eligible_source_content_ids.sql")
    top100_df = client.query(query_top_100, job_config=query_config).to_dataframe()

    related_links_csv_writer = RelatedLinksCsv(top100_df, related_links.excluded_target_content_ids,
                                               related_links.model)
    related_links_csv_writer.write_to_csv(os.path.join(DATA_DIR, "predictions",
                                                       datetime.today().strftime(
                                                           '%Y%m%d') + "top100_suggested_related_links.json"))

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
