# Use our saved down node2vec model to get recs for each content ID
# Make sure to add exclusions - smart answers, simple smart answers,
# fatality notices, etc. + a file in the repo with a list of hardcoded
# specific exclusions
#
# Use document types from content store data?
# Does it break if there aren't 1000 recs for each link? maybe add a try
# around that to catch any exceptions

import logging.config
import os
import json

from datetime import datetime

from gensim.models import Word2Vec

from src.utils.big_query_client import BigQueryClient
from src.utils.miscellaneous import load_pickled_content_id_list
from src.utils.related_links_csv_exporter import RelatedLinksCsvExporter
from src.utils.related_links_json_exporter import RelatedLinksJsonExporter
from src.utils.related_links_predictor import RelatedLinksPredictor
from src.utils.related_links_confidence_filter import RelatedLinksConfidenceFilter
from src.utils.date_helper import DateHelper


def get_content_id_to_base_path_mapper(path):
    with open(path, 'r') as content_id_to_base_path_mapping_file:
        return json.load(content_id_to_base_path_mapping_file)


def get_content_ids_to_page_views_mapper(df):
    """
    Transform BigQuery dataframe to a dictionary where keys are content_ids and the values are pageviews.
    :param df:
    :return:
    """
    return df.set_index('content_id').T.to_dict('records')[0]


if __name__ == '__main__':
    logging.config.fileConfig('src/logging.conf')
    logger = logging.getLogger('predict_related_links')
    DATA_DIR = os.getenv("DATA_DIR")

    related_links_path = os.path.join(DATA_DIR, "predictions",
                                      datetime.today().strftime('%Y%m%d') + "suggested_related_links")
    content_id_base_mapping_path = os.path.join(DATA_DIR, 'tmp', 'content_id_base_path_mapping.json')

    logger.info(
        f'loading model from "models/n2v.model"')
    trained_model = Word2Vec.load("models/n2v.model")

    logger.info(
        f'loading eligible_source_content_ids from {DATA_DIR}/tmp/eligible_source_content_ids.pkl')
    eligible_source_content_ids = load_pickled_content_id_list(os.path.join(DATA_DIR, "tmp",
                                                                            "eligible_source_content_ids.pkl"))

    eligible_target_content_ids = load_pickled_content_id_list(os.path.join(DATA_DIR, "tmp",
                                                                            "eligible_target_content_ids.pkl"))

    logger.info('Querying Big Query for content ids and views')
    yesterday = DateHelper.get_datetime_for_yesterday()
    three_weeks_ago = DateHelper.get_datetime_for_days_ago(22)

    bq_client = BigQueryClient()
    all_content_ids_and_views_df = bq_client.query_page_views(three_weeks_ago, yesterday)

    pageview_confidence_config = {
        100: 0.90,
        500: 0.65,
    }
    related_links_filter = RelatedLinksConfidenceFilter(
        get_content_ids_to_page_views_mapper(all_content_ids_and_views_df), pageview_confidence_config)

    logger.info(f'predicting related links')
    related_links_predictor = RelatedLinksPredictor(eligible_source_content_ids, eligible_target_content_ids,
                                                    trained_model, related_links_filter)
    related_links = related_links_predictor.predict_all_related_links()

    logger.info('Exporting related links as JSON')
    json_exporter = RelatedLinksJsonExporter(related_links)
    json_exporter.export(f'{related_links_path}.json')

    logger.info('Filtering to content_ids in the vocabulary')
    all_content_ids_and_views_df.query(
        'content_id in @trained_model.wv.vocab.keys() and content_id in @eligible_source_content_ids',
        inplace=True)

    logger.info('Exporting top 100 pages as CSV')
    csv_exporter = RelatedLinksCsvExporter(related_links,
                                           get_content_id_to_base_path_mapper(content_id_base_mapping_path),
                                           get_content_ids_to_page_views_mapper(all_content_ids_and_views_df))
    csv_exporter.export(f'{related_links_path}.tsv')

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
