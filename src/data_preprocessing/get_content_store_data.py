import json
import os
import warnings
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import pymongo
from tqdm import tqdm
import pickle
from src.utils.miscellaneous import read_config_yaml, safe_getenv
from src.utils import text_preprocessing as tp

warnings.filterwarnings('ignore', category=UserWarning, module='bs4')

tqdm.pandas()

KEYS_FOR_LINK_TYPES = {
    "related": "ordered_related_items",
    "collection": "documents"
}

BLOCKLIST_DOCUMENT_TYPES = read_config_yaml(
    "document_types_excluded_from_the_topic_taxonomy.yml")['document_types']

EXCLUDED_SOURCE_CONTENT = read_config_yaml("source_exclusions_that_are_not_linked_from.yml")
EXCLUDED_TARGET_CONTENT = read_config_yaml("target_exclusions_that_are_not_linked_to.yml")

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
        expanded_links.documents) NB doesn't consider suggested_ordered_related_items
        (the algorithmically generated links)
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
    TODO There isn't a 1:1 relationship with content ids as paths, as this assumes. e.g multiple language versions.
    :param mongodb_collection:
    :return: Python dictionary {page_path: content_id}, Python dictionary {content_id: base_path}
    """
    print('querying MongoDB for base_paths, slugs, and content_ids')
    base_path_content_id_cursor = mongodb_collection.find(
        {"$and": [
            {"content_id": {"$exists": True}},
            {"phase": "live"}]},
        {"content_id": 1,
         "details.parts.slug": 1,
         "locale": 1})
    page_path_content_id_mapping = dict()
    content_id_base_path_mapping = dict()
    for item in base_path_content_id_cursor:
        page_path_content_id_mapping.update({item['_id']: item['content_id']})
        if item['locale'] == "en":
            content_id_base_path_mapping.update({item['content_id']: item['_id']})
        for part in item.get('details', {}).get('parts', []):
            page_path_content_id_mapping.update(
                {os.path.join(item['_id'], part['slug']): item['content_id']})
    print(f'len(page_path_content_id_mapping): {len(page_path_content_id_mapping)}')
    print(f'len(content_id_base_path_mapping): {len(content_id_base_path_mapping)}')
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
    #    print(f'df with details text has columns={list(df.columns)} and shape={df.shape}')
    print('df with details text done')
    return df[['_id', 'content_id', 'all_details']]


def reshape_df_explode_list_column(wide_df, list_column):
    """
    Bit like a melt, we have a list column in a DataFrame, and we repeat all other columns for each item in the list
    TODO: would be nice to bump pandas and call DataFrame.explode, but it breaks other stuff
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
    print('have applied extract_links_from_content_details to page_text_df')

    embedded_links_df = page_text_df[['_id', 'content_id', 'embedded_links']]
    print(f'shape of df with link list (wide before melt)={embedded_links_df.shape}')

    embedded_links_df = reshape_df_explode_list_column(embedded_links_df, 'embedded_links')
    print(f'shape of df after melt (each link in its own row)={embedded_links_df.shape}')

    embedded_links_df['embedded_links'] = embedded_links_df['embedded_links'].apply(tp.clean_page_path)
    embedded_links_df['destination_content_id'] = embedded_links_df['embedded_links'].map(
        base_path_to_content_id_mapping)
    print('mapping of page_path to content_id has completed')

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
    print(f'related links dataframe shape {related_links_df.shape}')

    collection_links_df = convert_link_list_to_df(get_links(mongodb_collection, 'collection'), 'collection')
    print(f'collection links dataframe shape {collection_links_df.shape}')

    page_text_df = get_page_text_df(mongodb_collection)

    print('extracting embedded links')

    embedded_links_df = extract_embedded_links_df(page_text_df, page_path_content_id_mapping)
    print(f'embedded links dataframe shape {embedded_links_df.shape}')

    structural_edges_df = pd.concat(
        [related_links_df, collection_links_df, embedded_links_df],
        axis=0, sort=True, ignore_index=True)

    print(f'structural edges dataframe shape {structural_edges_df.shape}')

    # filter out any links without a destination content ID, as we are building a network based on content_ids
    structural_edges_df.query('destination_content_id.notnull()', inplace=True)
    print(
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
                                   {"content_id": {"$in": specific_excluded_target_content_ids}},
                                   {"locale": {"$ne": "en"}}]}

    # TODO simplify this. Loop through cursor instead?
    content_ids_list_of_dicts = list(
        mongodb_collection.find(mongodb_filter, {"content_id": 1, '_id': 0}))
    content_ids_and_nones_list = [content_id.get('content_id') for content_id in content_ids_list_of_dicts]
    content_ids_list = list(filter(None, set(content_ids_and_nones_list)))

    with open(outfile, 'wb') as fp:
        pickle.dump(content_ids_list, fp)

    return content_ids_list


# if __name__ == "__main__":  # our module is being executed as a program
def run(mongodb_uri, data_dir):

    print('starting get_content_store_data')
    print(f'content store uri: {mongodb_uri}')
    print(f'data directory: {data_dir}')

    preprocessing_config = read_config_yaml("preprocessing-config.yml")
    #    data_dir = preprocessing_config["data_dir"]

    # output files
    content_id_base_path_mapping_filename = os.path.join(data_dir, 'content_id_base_path_mapping.json')
    page_path_content_id_mapping_filename = os.path.join(data_dir, 'page_path_content_id_mapping.json')
    eligible_source_content_ids_filename = os.path.join(data_dir, 'eligible_source_content_ids.pkl')
    eligible_target_content_ids_filename = os.path.join(data_dir, 'eligible_target_content_ids.pkl')
    structural_edges_output_filename = os.path.join(data_dir, preprocessing_config['structural_edges_filename'])

    # Input: Mongo database
    mongo_client = pymongo.MongoClient(mongodb_uri)
    content_store_db = mongo_client.get_default_database()
    content_store_collection = content_store_db['content_items']

    page_path_content_id_mapping, content_id_base_path_mapping = get_path_content_id_mappings(content_store_collection)

    print(f'saving page_path_content_id_mapping to {page_path_content_id_mapping_filename}')
    with open(page_path_content_id_mapping_filename, 'w') as page_path_content_id_file:
        json.dump(page_path_content_id_mapping, page_path_content_id_file)

    print(f'saving content_id_base_path_mapping to {content_id_base_path_mapping_filename}')
    with open(content_id_base_path_mapping_filename, 'w') as content_id_base_path_file:
        json.dump(content_id_base_path_mapping, content_id_base_path_file)

    output_df = get_structural_edges_df(content_store_collection, page_path_content_id_mapping)

    print(
        f'saving structural_edges (output_df) to {structural_edges_output_filename}')
    output_df.to_csv(structural_edges_output_filename, index=False)

    export_content_id_list("eligible_source",
                           content_store_collection,
                           eligible_source_content_ids_filename)

    export_content_id_list("eligible_target",
                           content_store_collection,
                           eligible_target_content_ids_filename)
