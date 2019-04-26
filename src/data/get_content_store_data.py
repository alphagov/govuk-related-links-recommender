import os
import yaml

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize


document_types_excluded_from_the_topic_taxonomy_filename = \
    os.path.join(os.path.abspath(os.path.dirname(__file__)),
                 '..', 'config', 'document_types_excluded_from_the_topic_taxonomy.yml'
    )


def document_types_excluded_from_the_topic_taxonomy():
    with open(
        document_types_excluded_from_the_topic_taxonomy_filename,
        'r'
    ) as f:
        return yaml.load(f)['document_types']


BLACKLIST_DOCUMENT_TYPES = document_types_excluded_from_the_topic_taxonomy()

LABELLED_LINKS_PROJECTION = {
    "expanded_links.ordered_related_items.base_path": 1,
    "expanded_links.documents.base_path": 1,
    "expanded_links.ordered_related_items.content_id": 1,
    "expanded_links.documents.content_id": 1,
    "content_id": 1}

FILTER_RELATED_LINKS = {
    "$and": [{"expanded_links.ordered_related_items": {"$exists": True}},
             {"document_type": {"$nin": BLACKLIST_DOCUMENT_TYPES}},
             {"phase": "live"}]}

FILTER_COLLECTION_LINKS = { "$and": [{"expanded_links.documents": {"$exists": True}},
                    { "document_type": {"$nin": BLACKLIST_DOCUMENT_TYPES}},
                    { "phase": "live"}]}


def get_related_links(mongodb_collection):
    return list(mongodb_collection.find(FILTER_RELATED_LINKS, LABELLED_LINKS_PROJECTION))


def get_collection_links(mongodb_collection):
    return list(mongodb_collection.find(FILTER_COLLECTION_LINKS, LABELLED_LINKS_PROJECTION))


def is_html(text):
    try:
        return bool(BeautifulSoup(text, "html.parser").find())
    # TODO: do no use bare 'except'
    except:
        pass


def extract_html_links(text):
    """
    Grab any GOV.UK domain-specific links from page text.
    :param text: Text within a details sub-section, refer to filtered for keys.
    :return: list of links
    """
    links = []
    try:
        soup = BeautifulSoup(text, "html5lib")
        links = [link.get('href') for link in soup.findAll('a', href=True)]
    # TODO: do we need this except? it's too broad as is
    except Exception:
        # print("error")
        None
    return [l.replace("https://www.gov.uk/", "/") for l in links
            if l.startswith("/") or l.startswith("https://www.gov.uk/")]


def extract_html(cell_contents, links=[]):

    if type(cell_contents) == list:
        [extract_html(item, links) for item in cell_contents]

    elif type(cell_contents) == dict:
        extract_html(list(cell_contents.values()), links)

    else:
        if is_html(cell_contents):
            links.extend(extract_html_links(cell_contents))

    return links


def get_all_links_df(mongodb_collection):
    related_links_df = json_normalize(get_related_links(mongodb_collection),
                                      record_path=[['expanded_links', 'ordered_related_items']],
                                      meta=['_id', 'content_id'],
                                      meta_prefix='source_'
                                      )
    related_links_df.columns = ['destination_base_path', 'destination_content_id', 'source_base_path',
                                'source_content_id']
    related_links_df['link_type'] = 'related_link'

    collection_links_df = json_normalize(get_collection_links(mongodb_collection),
                                         record_path=[['expanded_links', 'documents']],
                                         meta=['_id', 'content_id'],
                                         meta_prefix='source_'
                                         )
    collection_links_df.columns = ['destination_base_path', 'destination_content_id', 'source_base_path',
                                   'source_content_id']
    collection_links_df['link_type'] = 'collection_link'

    base_path_content_id_cursor = mongodb_collection.find({"$and": [
        {"content_id": {"$exists": True}},
        {"phase": "live"}]},
        {"content_id": 1})
    base_path_to_content_id_lookup_dict = {item['_id']: item['content_id'] for item in base_path_content_id_cursor}

    filter_basic = {"$and": [
        {"document_type": {"$nin": BLACKLIST_DOCUMENT_TYPES}},
        {"phase": "live"}]}
    text_projection = {
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
    text_cursor = mongodb_collection.find(filter_basic, text_projection)
    text_list = list(text_cursor)

    text_df = json_normalize(text_list)
    text_df['all_details'] = text_df.iloc[:, 2:-1].values.tolist()
    text_df['embedded_links'] = text_df.apply(
        lambda row: extract_html(
            cell_contents=row['all_details'],
            links=[]),
        axis=1)

    embedded_links_df = text_df[['_id', 'content_id', 'embedded_links']]
    print(embedded_links_df.shape)

    lst_col = 'embedded_links'
    print(embedded_links_df.sort_values('embedded_links', ascending=False).head())

    embedded_links_df_2 = pd.DataFrame({
        col: np.repeat(embedded_links_df[col].values, embedded_links_df[lst_col].str.len())
        for col in embedded_links_df.columns.difference([lst_col])
    })
    print(embedded_links_df_2.shape)

    def keep_first_part_of_basepath(basepath):
        return (os.path.split(basepath))[0]

    embedded_links_df['first_part_path'] = embedded_links_df['embedded_links'].apply(keep_first_part_of_basepath)
    embedded_links_df['first_part_path2'] = embedded_links_df['first_part_path'].apply(keep_first_part_of_basepath)

    embedded_links_df['destination_content_id'] = embedded_links_df['embedded_links'].map(
        base_path_to_content_id_lookup_dict)
    embedded_links_df['destination_content_id2'] = embedded_links_df['first_part_path'].map(
        base_path_to_content_id_lookup_dict)
    embedded_links_df['destination_content_id3'] = embedded_links_df['first_part_path2'].map(
        base_path_to_content_id_lookup_dict)
    embedded_links_df['final'] = embedded_links_df['destination_content_id'].fillna(
        embedded_links_df['destination_content_id2'])
    embedded_links_df['final'] = embedded_links_df['final'].fillna(embedded_links_df['destination_content_id3'])

    embedded_links_df.drop(['destination_content_id',
                            'first_part_path', 'destination_content_id2', 'first_part_path2',
                            'destination_content_id3'], axis=1, inplace=True)
    embedded_links_df.columns = ['source_base_path', 'source_content_id', 'destination_base_path',
                                 'destination_content_id']

    embedded_links_df['link_type'] = 'embedded_link'
    print(related_links_df.shape)
    print(collection_links_df.shape)
    print(embedded_links_df.shape)
    all_links = pd.concat([related_links_df, collection_links_df, embedded_links_df], axis=0, sort=True)
    return all_links
