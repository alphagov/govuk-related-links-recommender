import json
import logging.config
import os
import warnings
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import pymongo
from tqdm import tqdm
import pickle

from src.utils import text_preprocessing as tp

from src.utils.miscellaneous import read_exclusions_yaml, safe_getenv

warnings.filterwarnings('ignore', category=UserWarning, module='bs4')

tqdm.pandas()


KEYS_FOR_LINK_TYPES = {
    "related": "ordered_related_items",
    "collection": "documents"
}

BLOCKLIST_DOCUMENT_TYPES = read_exclusions_yaml(
    "document_types_excluded_from_the_topic_taxonomy.yml")['document_types']
EXCLUDED_SOURCE_CONTENT = read_exclusions_yaml("source_exclusions_that_are_not_linked_from.yml")
EXCLUDED_TARGET_CONTENT = read_exclusions_yaml("target_exclusions_that_are_not_linked_to.yml")

RELATED_LINKS_PROJECTION = {
    "expanded_links.ordered_related_items.base_path": 1,
    "expanded_links.ordered_related_items.content_id": 1,
    "content_id": 1}

COLLECTION_LINKS_PROJECTION = {
    "expanded_links.documents.base_path": 1,
    "expanded_links.documents.content_id": 1,
    "content_id": 1}

TEXT_PROJECTION = {
    "details.body": 1,
    "details.brand": 1,  # no documents found?
    "details.documents": 1,
    "details.final_outcome_detail": 1,
    "details.final_outcome_documents": 1,
    "details.government": 1,
    "details.headers": 1,
    "details.introduction": 1,
    "details.introductory_paragraph": 1,
    "details.licence_overview": 1,
    "details.licence_short_description": 1,
    "details.logo": 1,
    "details.metadata": 1,
    "details.more_information": 1,
    "details.need_to_know": 1,
    "details.other_ways_to_apply": 1,
    "details.summary": 1,
    "details.ways_to_respond": 1,
    "details.what_you_need_to_know": 1,
    "details.will_continue_on": 1,
    "details.parts": 1,
    "details.collection_groups": 1,
    "details.transaction_start_link": 1,
    "content_id": 1}

FILTER_BASIC = {"$and": [{"document_type": {"$nin": BLOCKLIST_DOCUMENT_TYPES}},
                         {"phase": "live"}]}

FILTER_RELATED_LINKS = {"$and": [{"expanded_links.ordered_related_items": {"$exists": True}},
                                 {"document_type": {"$nin": BLOCKLIST_DOCUMENT_TYPES}},
                                 {"phase": "live"}]}

FILTER_COLLECTION_LINKS = {"$and": [{"expanded_links.documents": {"$exists": True}},
                                    {"document_type": {"$nin": BLOCKLIST_DOCUMENT_TYPES}},
                                    {"phase": "live"}]}

OUTPUT_DF_COLUMNS = ['destination_base_path', 'destination_content_id', 'source_base_path', 'source_content_id']


def get_links(mongodb_collection, link_type):
    """
    Querying a MongoDB collection and returning a list of content items and their links, of the type link_type, from
        the expanded_links field
    :param mongodb_collection:
    :param link_type: either 'related' (looks in expanded_links.ordered_related_items)  or 'collection' (looks in
        expanded_links.documents)
    :return: list of content items identified by content ID and base_path, and their links, of the type link_type, from
        the expanded_links field (content IDs and base_paths)
    """
    if link_type == 'related':
        return list(mongodb_collection.find(FILTER_RELATED_LINKS, RELATED_LINKS_PROJECTION))
    elif link_type == 'collection':
        return list(mongodb_collection.find(FILTER_COLLECTION_LINKS, COLLECTION_LINKS_PROJECTION))
    else:
        raise ValueError('link_type should either be "related" or "collection"')


def convert_link_list_to_df(link_list, link_type, columns=OUTPUT_DF_COLUMNS):
    """
    Converts a related or collection link list to a pandas DataFrame, with columns labelled consistently for
        onward pipeline
    :param link_list: list of a pymongo cursor, created using get_links
    :param link_type: either 'related' (from expanded_links.ordered_related_items)  or 'collection' (from
        expanded_links.documents)
    :param columns: column names for the resulting dataframe
    :return: pandas DataFrame containing source and destination page_paths and content_ids for the specified link_type
    """
    try:
        link_key = KEYS_FOR_LINK_TYPES[link_type]
    except KeyError:
        raise ValueError(
            f'link_type should be one of {KEYS_FOR_LINK_TYPES.keys()}')
    df = json_normalize(link_list,
                        record_path=[['expanded_links', link_key]],
                        meta=['_id', 'content_id'],
                        meta_prefix='source_')
    df.columns = columns
    df['link_type'] = f'{link_type}_link'
    return df


def get_path_content_id_mappings(mongodb_collection):
    """
    Queries a MongoDB collection and creates mappings of page_paths (base_paths with slugs), base_paths and content_ids
    :param mongodb_collection:
    :return: Python dictionary {page_path: content_id}, Python dictionary {content_id: base_path}
    """
    logging.info(f'querying MongoDB for base_paths, slugs, and content_ids')
    base_path_content_id_cursor = mongodb_collection.find({"$and": [
        {"content_id": {"$exists": True}},
        {"phase": "live"}]},
        {"content_id": 1,
         "details.parts.slug": 1})
    page_path_content_id_mapping = dict()
    content_id_base_path_mapping = dict()
    for item in base_path_content_id_cursor:
        page_path_content_id_mapping.update({item['_id']: item['content_id']})
        content_id_base_path_mapping.update({item['content_id']: item['_id']})
        for part in item.get('details', {}).get('parts', []):
            page_path_content_id_mapping.update(
                {os.path.join(item['_id'], part['slug']): item['content_id']})
    logging.info(f'len(page_path_content_id_mapping): {len(page_path_content_id_mapping)}')
    logging.info(f'len(content_id_base_path_mapping): {len(content_id_base_path_mapping)}')
    return page_path_content_id_mapping, content_id_base_path_mapping


def get_page_text_df(mongodb_collection):
    """
    Queries a MongoDB collection, get specific fields from details using TEXT_PROJECTION, converts this cursor to a
        DataFrame, with all details fields in one list column
    :param mongodb_collection:
    :return: pandas DataFrame with: _id (base_path), content_id, and all_details list column
    """
    text_list = list(mongodb_collection.find(FILTER_BASIC, TEXT_PROJECTION))
    df = json_normalize(text_list)
    # concatenate text from all columns (except first 2) into a list -> so we get a list of all the details fields
    # that we queried
    df['all_details'] = df.iloc[:, 2:-1].values.tolist()
    logging.info(f' df with details text has columns={list(df.columns)} and shape={df.shape}')
    return df[['_id', 'content_id', 'all_details']]


def reshape_df_explode_list_column(wide_df, list_column):
    """
    Bit like a melt, we have a list column in a DataFrame, and we repeat all other columns for each item in the list
    :param wide_df: pandas DataFrame with a list column
    :param list_column: list column name
    :return: DataFrame with one row per item in the list_column
    """
    # repeat all columns except list_col as many times as the list is long for that row
    # get a 1D vecotr using concatenate to flatten all values in list vector
    # and unpack this vector into a new column called list_col
    return pd.DataFrame({
        col: np.repeat(wide_df[col].values, wide_df[list_column].str.len())
        for col in wide_df.columns.difference([list_column])
    }).assign(**{list_column: np.concatenate(wide_df[list_column].values)})[
        wide_df.columns.tolist()]


def extract_embedded_links_df(page_text_df, base_path_to_content_id_mapping):
    """
    Takes a dataframe with  a list column (all_details), returns a dataframe with one in-page (embedded) link per row
    :param page_text_df: pandas DataFrame with  a list column (all_details)
    :param base_path_to_content_id_mapping: Python dictionary {page_path: content_id}
    :return:  pandas DataFrame  of embedded links with columns ['source_base_path', 'source_content_id',
        'destination_base_path','destination_content_id', 'link_type']
    """
    page_text_df['embedded_links'] = page_text_df['all_details'].progress_apply(tp.extract_links_from_content_details)
    logging.info(f'have applied extract_links_from_content_details to page_text_df')

    embedded_links_df = page_text_df[['_id', 'content_id', 'embedded_links']]
    logging.info(f'shape of df with link list (wide before melt)={embedded_links_df.shape}')

    embedded_links_df = reshape_df_explode_list_column(embedded_links_df, 'embedded_links')
    logging.info(f'shape of df after melt (each link in its own row)={embedded_links_df.shape}')

    embedded_links_df['embedded_links'] = embedded_links_df['embedded_links'].apply(tp.clean_page_path)
    embedded_links_df['destination_content_id'] = embedded_links_df['embedded_links'].map(
        base_path_to_content_id_mapping)
    logging.info(f'mapping of page_path to content_id has completed')

    embedded_links_df.rename(
        columns={
            '_id': 'source_base_path',
            'content_id': 'source_content_id',
            'embedded_links': 'destination_base_path'},
        inplace=True)

    embedded_links_df['link_type'] = 'embedded_link'
    return embedded_links_df


def get_structural_edges_df(mongodb_collection, page_path_content_id_mapping):
    """
    Gets related, collection, and embedded links for all items in the mongodb collection
    :param mongodb_collection:
    :param page_path_content_id_mapping: Python dictionary {page_path: content_id}
    :return: pandas DataFrame with columns ['source_base_path', 'source_content_id', 'destination_base_path',
                                 'destination_content_id', 'link_type']
    """
    related_links_df = convert_link_list_to_df(get_links(mongodb_collection, 'related'), 'related')
    logging.info(f'related links dataframe shape {related_links_df.shape}')

    collection_links_df = convert_link_list_to_df(get_links(mongodb_collection, 'collection'), 'collection')
    logging.info(f'collection links dataframe shape {collection_links_df.shape}')

    page_text_df = get_page_text_df(mongodb_collection)

    embedded_links_df = extract_embedded_links_df(page_text_df, page_path_content_id_mapping)
    logging.info(f'embedded links dataframe shape {embedded_links_df.shape}')

    structural_edges_df = pd.concat(
        [related_links_df, collection_links_df, embedded_links_df],
        axis=0, sort=True, ignore_index=True)

    logging.info(f'structural edges dataframe shape {structural_edges_df.shape}')

    # filter out any links without a destination content ID, as we are building a network based on content_ids
    structural_edges_df.query('destination_content_id.notnull()', inplace=True)
    logging.info(
        f'structural edges dataframe shape f after dropping null destination_content_ids={structural_edges_df.shape}')
    return structural_edges_df


def export_content_id_list(list_name, mongodb_collection, outfile):
    """
    Queries the MongoDB collection to return a list of content_ids that meet the criteria set out in the YAML
    config files: either including content items as source pages or including them as target pages
    :param list_name: string either "eligible_source" or "included_target"
    :param mongodb_collection:
    :param outfile: file path for the saved pickled list of content ids
    :return: list of content_ids, either representing those eligible as source pages or
    those eligible as target pages
    """

    # TODO: We can tidy this up later on as the two ifs are very similar, so a candidate we could extract to a method
    #  (in addition to ensuring that only eligible_source and excluded_target are the only two valid list names (if
    #  something else is passed in, throw an exception)).
    if list_name == 'eligible_source':
        specific_excluded_source_content_ids = EXCLUDED_SOURCE_CONTENT['content_ids']
        excluded_source_document_types = EXCLUDED_SOURCE_CONTENT['document_types']

        mongodb_filter = {"$and": [{"expanded_links.ordered_related_items": {"$exists": False}},
                                   {"document_type": {"$nin": BLOCKLIST_DOCUMENT_TYPES}},
                                   {"document_type": {"$nin": excluded_source_document_types}},
                                   {"content_id": {"$nin": specific_excluded_source_content_ids}},
                                   {"phase": "live"}]}
    if list_name == 'eligible_target':
        specific_excluded_target_content_ids = EXCLUDED_TARGET_CONTENT['content_ids']
        excluded_target_document_types = EXCLUDED_TARGET_CONTENT['document_types']

        mongodb_filter = {"$nor": [{"expanded_links.ordered_related_items": {"$exists": True}},
                                   {"document_type": {"$in": BLOCKLIST_DOCUMENT_TYPES}},
                                   {"document_type": {"$in": excluded_target_document_types}},
                                   {"content_id": {"$in": specific_excluded_target_content_ids}}]}

    # TODO simplify this. Loop through cursor instead?
    content_ids_list_of_dicts = list(
        mongodb_collection.find(mongodb_filter, {"content_id": 1, '_id': 0}))
    content_ids_and_nones_list = [content_id.get('content_id') for content_id in content_ids_list_of_dicts]
    content_ids_list = list(filter(None, content_ids_and_nones_list))

    with open(outfile, 'wb') as fp:
        pickle.dump(content_ids_list, fp)

    return content_ids_list


if __name__ == "__main__":  # our module is being executed as a program
    data_dir = safe_getenv('DATA_DIR')

    logging.config.fileConfig('src/logging.conf')
    module_logger = logging.getLogger('get_content_store_data')

    mongo_client = pymongo.MongoClient("mongodb://content-store/")
    # TODO check this is consistent with naming of restored db in AWS
    content_store_db = mongo_client['content_store']
    content_store_collection = content_store_db['content_items']

    page_path_content_id_mapping, content_id_base_path_mapping = get_path_content_id_mappings(content_store_collection)

    module_logger.info(f'saving page_path_content_id_mapping to {data_dir}/tmp/page_path_content_id_mapping.json')
    with open(
            os.path.join(data_dir, 'tmp', 'page_path_content_id_mapping.json'),
            'w') as page_path_content_id_file:
        json.dump(page_path_content_id_mapping, page_path_content_id_file)

    module_logger.info(f'saving content_id_base_path_mapping to {data_dir}/tmp/content_id_base_path_mapping.json')
    with open(
            os.path.join(data_dir, 'tmp', 'content_id_base_path_mapping.json'),
            'w') as content_id_base_path_file:
        json.dump(content_id_base_path_mapping, content_id_base_path_file)

    output_df = get_structural_edges_df(content_store_collection, page_path_content_id_mapping)

    module_logger.info(f'saving structural_edges (output_df) to {data_dir}/tmp/structural_edges.json')
    output_df.to_csv(os.path.join(data_dir, "tmp", "structural_edges.csv"), index=False)

    export_content_id_list("eligible_source",
                           content_store_collection,
                           os.path.join(data_dir, "tmp", "eligible_source_content_ids.pkl"))

    export_content_id_list("eligible_target",
                           content_store_collection,
                           os.path.join(data_dir, "tmp",
                                        "eligible_target_content_ids.pkl"))


# This is code from a colleague's blog, with an alternative way of doing this, that we need to compare efficiency with.
# We haven't done it yet, because for MVP we're sticking with what we have already made work for our use case (getting
# on-page/embedded links is the tricky thing not tackled by this code).
# https://memo.barrucadu.co.uk/mapping-govuk.html

# #! /usr/bin/env nix-shell
# #! nix-shell -i python3 --packages "python3.withPackages(ps: [ps.pymongo])"
#
# from pymongo import MongoClient
# import csv
# import os
# import sys
#
# ALL_LINKS_CATEGORY = 'all-links'
# LINK_CATEGORIES = {
#     'organisations': ['lead_organisations', 'ordered_child_organisations', 'ordered_high_profile_groups',
#       'ordered_parent_organisation', 'ordered_successor_organisations', 'organisations', 'supporting_organisations',
#       'worldwide_organisations'],
#     'people': ['ministers', 'people', 'speaker'],
#     'publishing-organisations': ['original_primary_publishing_organisation', 'primary_publishing_organisation'],
#     'step-by-step': ['pages_part_of_step_nav', 'pages_related_to_step_nav', 'part_of_step_navs',
#       'related_to_step_navs'],
#     'taxonomy': ['alpha_taxons', 'associated_taxons', 'child_taxons', 'legacy_taxons', 'level_one_taxons',
#       'parent_taxons', 'root_taxon', 'taxons', 'topic_taxonomy_taxons'],
# }
#
# if 'SHOW_CATEGORIES' in os.environ:
#     print(ALL_LINKS_CATEGORY)
#     for category in LINK_CATEGORIES.keys():
#         print(category)
#     sys.exit(0)
#
# MONGO_URL = os.environ['MONGO_URL']
# CSV_FILE = os.environ.get('CSV_FILE', 'links.csv')
# LINK_CATEGORY = os.environ.get('LINK_CATEGORY', ALL_LINKS_CATEGORY)
#
#
# def includes(linkty):
#     if LINK_CATEGORY == ALL_LINKS_CATEGORY:
#         return True
#     return linkty in LINK_CATEGORIES[LINK_CATEGORY]
#
#
# documents = MongoClient(MONGO_URL).content_store['content_items'].find({})
#
# with open(CSV_FILE, 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#     for document in documents:
#         links = []
#         for linkty, links_of_type in document.get('expanded_links', {}).items():
#             if includes(linkty):
#                 links.extend(link['base_path'] for link in links_of_type if 'base_path' in link)
#         if links != []:
#             writer.writerow([document['_id']] + links)
