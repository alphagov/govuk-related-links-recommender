import json
import logging.config
import os

import pandas as pd

# link types we want to use for our structural network
STRUCTURAL_LINK_TYPES = ['embedded_links',  'collection_links', 'related_links']
logging.config.fileConfig('src/logging.conf')


# TODO: come back to this and generalise fn to handle both structural and functional networks and convert to edges
def make_structural_network_and_node_id_mappings(all_links_df):
    """
    Takes a DataFrame pages and their links as content_ids, returns a DataFrame of these edges using node_ids,
    and mappings between the node_ids and content_ids
    :param all_links_df: pandas DataFrame with columns source_content_id and destination_content_id
    (content_ids of pages, and pages they link to)
    :return: pandas DataFrame of edges using node_ids, Python dict {content_id: node_id}, Python dict {node_id: content_id}
    """
    logger = logging.getLogger('make_structural_network.make_structural_network_and_node_id_mappings')

    content_id_node_id_mapping = {}
    num_counter = 0

    logger.info('assigning node_ids to content_ids, to create content_id_node_id_mapping')
    # TODO: Not for now but when refactoring, to make more OOP, there is repetition in the loop
    #  that could be extracted out. Not needed for MVP but could be considered during refactor
    for row in all_links_df.itertuples(index=False):
        if row.source_content_id not in content_id_node_id_mapping.keys():
            content_id_node_id_mapping[row.source_content_id] = str(num_counter)
            num_counter += 1
        if row.destination_content_id not in content_id_node_id_mapping.keys():
            content_id_node_id_mapping[row.destination_content_id] = str(num_counter)
            num_counter += 1

    logger.info('adding source and target node_ids to all_links_df')
    all_links_df['source'] = all_links_df['source_content_id'].map(
        content_id_node_id_mapping)
    all_links_df['target'] = all_links_df['destination_content_id'].map(
        content_id_node_id_mapping)

    logger.info(
        'creating node_id_content_id_mapping from content_id_node_id_mapping')
    node_id_content_id_mapping = dict(
        (node_id, content_id) for content_id, node_id in content_id_node_id_mapping.items())

    return all_links_df, content_id_node_id_mapping, node_id_content_id_mapping


if __name__ == "__main__":  # our module is being executed as a program
    data_dir = os.getenv('DATA_DIR')
    module_logger = logging.getLogger('make_structural_network')

    module_logger.info(f'reading {data_dir}/tmp/all_links.csv created by data_preprocessing/get_content_store_data')
    input_df = pd.read_csv(os.path.join(data_dir, 'tmp',  'all_links.csv'))

    module_logger.info('making structural_network_df using all_links')
    structural_network_df, content_id_node_id_mapping_dict, node_id_content_id_mapping_dict = \
        make_structural_network_and_node_id_mappings(input_df)

    module_logger.info(f'saving structural_network_df to {data_dir}/tmp/structural_network.csv')
    structural_network_df.to_csv(os.path.join(
        data_dir, 'tmp', 'structural_network.csv'), index=False)

    module_logger.info(f'saving content_id_node_id_mapping_dict to {data_dir}/tmp/content_id_node_id_mapping.json')
    with open(
            os.path.join(data_dir, 'tmp', 'content_id_node_id_mapping.json'),
            'w') as content_id_node_id_file:
        json.dump(content_id_node_id_mapping_dict, content_id_node_id_file)

    module_logger.info(f'saving node_id_content_id_mapping_dict to {data_dir}/tmp/node_id_content_id_mapping.json')
    with open(
            os.path.join(data_dir, 'tmp', 'node_id_content_id_mapping.json'),
            'w') as node_id_content_id_file:
        json.dump(node_id_content_id_mapping_dict, node_id_content_id_file)