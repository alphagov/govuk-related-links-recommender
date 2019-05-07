import json
import logging.config
import os

import pandas as pd

# link types we want to use for our structural network
STRUCTURAL_LINK_TYPES = ['embedded_links',  'collection_links', 'related_links']
logging.config.fileConfig('src/logging.conf')


def make_structural_network_and_node_id_mappings(all_links_df):
    logger = logging.getLogger('make_structural_network.make_structural_network_and_node_id_mappings')
    # filter out any  inks without a destination content ID, as we are building a network based on content_ids
    logger.info(
        f'shape of all_links_df before dropping null destination_content_ids={all_links_df.shape}')
    all_links_df.query('destination_content_id.notnull()', inplace=True)
    logger.info(
        f'shape of all_links_df after dropping null  destination_content_ids={all_links_df.shape}')

    content_id_node_id_mapping = {}
    num_counter = 0

    logger.info('assigning node_ids to content_ids, to create content_id_node_id_mapping')
    for i, row in all_links_df.iterrows():
        if row['source_content_id'] not in content_id_node_id_mapping.keys():
            content_id_node_id_mapping[row['source_content_id']] = num_counter
            num_counter += 1
        if row['destination_content_id'] not in content_id_node_id_mapping.keys():
            content_id_node_id_mapping[row['destination_content_id']] = num_counter
            num_counter += 1

    all_links_df['source'] = 0
    all_links_df['target'] = 0

    logger.info('adding source and target node_ids to all_links_df')
    all_links_df['source'] = all_links_df['source_content_id'].map(
        content_id_node_id_mapping)
    all_links_df['target'] = all_links_df['destination_content_id'].map(
        content_id_node_id_mapping)

    logger.info(
        'creating node_id_content_id_mapping from content_id_node_id_mapping')
    node_id_content_id_mapping = dict(
        (v, k) for k, v in content_id_node_id_mapping.items())

    return all_links_df, content_id_node_id_mapping, node_id_content_id_mapping


if __name__ == "__main__":  # our module is being executed as a program
    datadir = os.getenv('DATADIR')
    module_logger = logging.getLogger('make_structural_network')

    module_logger.info(f'reading {datadir}/tmp/all_links.csv created by data_preprocessing/get_content_store_data')
    input_df = pd.read_csv(os.path.join(datadir, 'tmp',  'all_links.csv'))

    module_logger.info('making structural_network_df using all_links')
    structural_network_df, content_id_node_id_mapping_dict, node_id_content_id_mapping_dict = \
        make_structural_network_and_node_id_mappings(input_df)

    module_logger.info(f'saving structural_network_df to {datadir}/tmp/structural_network.csv')
    structural_network_df.to_csv(os.path.join(
        datadir, 'tmp', 'structural_network.csv'), index=False)

    module_logger.info(f'saving content_id_node_id_mapping_dict to {datadir}/tmp/content_id_node_id_mapping.json')
    with open(
            os.path.join(datadir, 'tmp', 'content_id_node_id_mapping.json'),
            'w') as content_id_node_id_file:
        json.dump(content_id_node_id_mapping_dict, content_id_node_id_file)

    module_logger.info(f'saving node_id_content_id_mapping_dict to {datadir}/tmp/node_id_content_id_mapping.json')
    with open(
            os.path.join(datadir, 'tmp', 'node_id_content_id_mapping.json'),
            'w') as node_id_content_id_file:
        json.dump(node_id_content_id_mapping_dict, node_id_content_id_file)