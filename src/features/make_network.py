import json
import logging.config
import os

import pandas as pd

# link types we want to use for our structural network
logging.config.fileConfig('src/logging.conf')


# TODO: come back to this and generalise fn to handle both structural and functional networks and convert to edges
def make_network_and_node_id_mappings(edges_df):
    """
    Takes a DataFrame pages and their links as content_ids, returns a DataFrame of these edges using node_ids,
    and mappings between the node_ids and content_ids
    :param edges_df: pandas DataFrame with columns source_content_id and destination_content_id
    (content_ids of pages, and pages they link to)
    :return: pandas DataFrame of edges using node_ids, Python dict {content_id: node_id}, Python dict {node_id: content_id}
    """
    # NB this function changes the existing edges_df too
    logger = logging.getLogger('make_structural_network.make_structural_network_and_node_id_mappings')

    content_id_node_id_mapping = {}
    num_counter = 0

    logger.info('assigning node_ids to content_ids, to create content_id_node_id_mapping')
    # TODO: Not for now but when refactoring, to make more OOP, there is repetition in the loop
    #  that could be extracted out. Not needed for MVP but could be considered during refactor
    for row in edges_df.itertuples(index=False):
        if row.source_content_id not in content_id_node_id_mapping.keys():
            content_id_node_id_mapping[row.source_content_id] = str(num_counter)
            num_counter += 1
        if row.destination_content_id not in content_id_node_id_mapping.keys():
            content_id_node_id_mapping[row.destination_content_id] = str(num_counter)
            num_counter += 1

    logger.info('adding source and target node_ids to all_links_df')
    edges_df['source_node'] = edges_df['source_content_id'].map(
        content_id_node_id_mapping)
    edges_df['target_node'] = edges_df['destination_content_id'].map(
        content_id_node_id_mapping)

    logger.info(
        'creating node_id_content_id_mapping from content_id_node_id_mapping')
    node_id_content_id_mapping = dict(
        (node_id, content_id) for content_id, node_id in content_id_node_id_mapping.items())

    return edges_df, content_id_node_id_mapping, node_id_content_id_mapping


def combine_structural_functional_edges(structural_edges, functional_edges):
    """
    Combine structural and functional dataframes to get a deduplicated dataframe of edges
    :param structural_edges: pandas DataFrame including columns ['source_content_id', 'destination_content_id']
    :param functional_edges: pandas DataFrame including columns ['source_content_id', 'destination_content_id']
    :return: pandas DataFrame with columns ['source_content_id', 'destination_content_id']
    """
    all_edges = pd.concat([structural_edges, functional_edges],
                          ignore_index=True, sort=True)
    return all_edges[['source_content_id', 'destination_content_id']].drop_duplicates().reset_index(drop=True)


if __name__ == "__main__":  # our module is being executed as a program
    data_dir = os.getenv('DATA_DIR')
    module_logger = logging.getLogger('make_structural_network')

    module_logger.info(
        f'reading {data_dir}/tmp/structural_edges.csv created by data_preprocessing/get_content_store_data')
    structural_edges_df = pd.read_csv(os.path.join(data_dir, 'tmp',  'structural_edges.csv'))

    module_logger.info(
        f'reading {data_dir}/tmp/functional_edges.csv.gz created by data_preprocessing/make_functional_edges_and_weights')
    functional_edges_df = pd.read_csv(
        os.path.join(data_dir, 'tmp', 'functional_edges.csv.gz'),
        compression='gzip')

    all_edges_df = combine_structural_functional_edges(structural_edges_df, functional_edges_df)

    all_edges_df.to_csv(os.path.join(
        data_dir, 'tmp', 'all_edges.csv'), index=False)

    module_logger.info('making network_df with node_ids using all_links')
    network_df, content_id_node_id_mapping_dict, node_id_content_id_mapping_dict = \
        make_network_and_node_id_mappings(all_edges_df)

    module_logger.info(f'saving network_df to {data_dir}/tmp/network.csv')
    network_df.to_csv(os.path.join(
        data_dir, 'tmp', 'network.csv'), index=False)

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
