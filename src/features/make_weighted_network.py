import logging.config
import os
import numpy as np
import pandas as pd
from src.utils.miscellaneous import read_config_yaml

# link types we want to use for our structural network
logging.config.fileConfig('src/logging.conf')


def make_weighted_network_from_structural_and_functional(structural_edges, functional_edges):
    """
    Combine structural and functional dataframes to get a deduplicated dataframe of edges
    with plus one smoothed edge weights
    Return unique source destination pairs with weight
    :param structural_edges: pandas DataFrame including columns ['source_content_id', 'destination_content_id']
    :param functional_edges: pandas DataFrame including columns ['source_content_id', 'destination_content_id']
    :return: pandas DataFrame with columns ['source_content_id', 'destination_content_id']
    """
    all_edges = pd.concat([structural_edges, functional_edges],
                          ignore_index=True, sort=True)

    # Add one to all edge weights so no edge has zero transition probability
    # Structural edges will therefore have a weight of 1
    # Functional edges will have a weight of n_users
    all_edges = all_edges.replace(np.nan, 0)
    all_edges['weight'] = all_edges['weight'] + 1

    # Deduplicate edges, retaining max edge weight (either 1 or functional edge weight)
    all_edges = all_edges.groupby(['source_content_id', 'destination_content_id'], as_index=False).aggregate(max)
    all_edges = all_edges[
        ['source_content_id', 'destination_content_id', 'weight']].reset_index(drop=True)
    return all_edges


if __name__ == "__main__":  # our module is being executed as a program
    data_dir = os.getenv('DATA_DIR')
    module_logger = logging.getLogger('making_network')

    preprocessing_config = read_config_yaml(
        "preprocessing-config.yml")

    module_logger.info(
        f"""reading {data_dir}/tmp/{preprocessing_config["structural_edges_filename"]}
            created by data_preprocessing/get_content_store_data""")
    structural_edges_df = pd.read_csv(os.path.join(data_dir, 'tmp', preprocessing_config["structural_edges_filename"]))
    module_logger.info(f'structural_edges_df.shape = {structural_edges_df.shape}')

    module_logger.info(
        f'reading {data_dir}/tmp/{preprocessing_config["functional_edges_filename"]} created by '
        f'data_preprocessing/make_functional_edges_and_weights')
    functional_edges_df = pd.read_csv(
        os.path.join(data_dir, 'tmp', preprocessing_config["functional_edges_filename"]))
    module_logger.info(f'functional_edges_df.shape = {functional_edges_df.shape}')

    module_logger.info('making network_df using structural_edges_df and functional_edges_df')
    network_df = make_weighted_network_from_structural_and_functional(structural_edges_df, functional_edges_df)
    module_logger.info(f'network_df.shape = {network_df.shape}')

    module_logger.info(f'saving network_df to {data_dir}/tmp/{preprocessing_config["network_filename"]}')
    network_df.to_csv(os.path.join(
        data_dir, 'tmp', preprocessing_config["network_filename"]), index=False)
