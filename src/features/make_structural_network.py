import logging.config
import re

# link types we want to use for our structural network
STRUCTURAL_LINK_TYPES = ['embedded_links',  'collection_links', 'related_links']


# def str_array_to_array(x):
#     return [l.strip() for l in re.sub(r"[\[\]']", "", x).split(",")]
#
#
# def get_links_for_each_page(all_links):
#     """Get the base_paths? contentIDs? of links we care about for each page (structural data of the site)
#
#     get links where their type is in TRUCTURAL_LINK_TYPES
#     :param all_links:
#     """
#     adj_list = []
#     for tup in all_links.itertuples(index=False):
#         for link_type in STRUCTURAL_LINK_TYPES:
#             tup_ind = all_links.columns.get_loc(link_type)
#             if len(tup[tup_ind]) > 0:
#                 for link_url in tup[tup_ind]:
#                     adj_list.append((tup.content_id, link_url, link_type))


def make_structural_network(all_links_df):
    # filter out any  links without a destination content ID, as we are building a network based on content_ids
    logging.info(f'''
        shape of all_links_df before dropping null 
        destination_content_ids={all_links_df.shape}
        ''')
    all_links_df.query('destination_content_id.notnull()', inplace=True)
    logging.info(f'''
        shape of all_links_df after dropping null 
        destination_content_ids={all_links_df.shape}
        ''')

    content_id_node_id_mapping = {}
    num_counter = 0

    for i, row in all_links_df.iterrows():
        if row['source_content_id'] not in content_id_node_id_mapping.keys():
            content_id_node_id_mapping[row['source_content_id']] = num_counter
            num_counter += 1
        if row['destination_content_id'] not in content_id_node_id_mapping.keys():
            content_id_node_id_mapping[row['destination_content_id']] = num_counter
            num_counter += 1

    all_links_df['source'] = 0
    all_links_df['target'] = 0

    all_links_df['source'] = all_links_df['source_content_id'].map(
        content_id_node_id_mapping)
    all_links_df['target'] = all_links_df['destination_content_id'].map(
        content_id_node_id_mapping)

    return all_links_df
