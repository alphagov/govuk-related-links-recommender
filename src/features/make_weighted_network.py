import logging.config
import os
import pandas as pd
from src.utils.miscellaneous import read_config_yaml, safe_getenv

# link types we want to use for our structural network
logging.config.fileConfig('src/logging.conf')


def make_weighted_network_from_structural_and_functional(structural_edges, functional_edges, structural_edge_weight):
    """
    Combine structural and functional dataframes to get a deduplicated dataframe of edges.
    Functional edges have a weight equal to the number of users that traversed that edge (excluding any below
    weight_threshold)
    Structural edges are set a weight of structural_edge_weight (so equivalent to structural_edge_weight users going
    between those two pages)
    Return unique source destination pairs with weight
    :param structural_edges: DataFrame including columns ['source_content_id', 'destination_content_id']
    :param functional_edges: DataFrame including columns ['source_content_id', 'destination_content_id', 'weight']
    :param structural_edge_weight: Integer that is assigned as the weight of structural edges
    :return: pandas DataFrame with columns ['source_content_id', 'destination_content_id','weight']
    """

    # Set weight of structural edges to structural_edge_weight
    structural_edges['weight'] = structural_edge_weight

    all_edges = pd.concat([structural_edges, functional_edges], ignore_index=True, sort=False)

    # Deduplicate edges, summing structural and functional edge weights
    all_edges = all_edges.groupby(['source_content_id', 'destination_content_id'], as_index=False).aggregate(sum)
    all_edges = all_edges[
        ['source_content_id', 'destination_content_id', 'weight']].reset_index(drop=True)
    return all_edges


if __name__ == "__main__":  # our module is being executed as a program

    data_dir = safe_getenv('DATA_DIR')
    preprocessing_config = read_config_yaml("preprocessing-config.yml")

    functional_edges_input_filename = os.path.join(data_dir, preprocessing_config["functional_edges_filename"])
    structural_edges_input_filename = os.path.join(data_dir, preprocessing_config["structural_edges_filename"])
    network_output_filename = os.path.join(data_dir, preprocessing_config["network_filename"])

    structural_edge_weight = preprocessing_config['structural_edge_weight']

    module_logger = logging.getLogger('making_network')

    module_logger.info(f'reading {structural_edges_input_filename}')
    structural_edges_df = pd.read_csv(structural_edges_input_filename)
    module_logger.info(f'structural_edges_df.shape = {structural_edges_df.shape}')

    module_logger.info(f'reading {functional_edges_input_filename}')
    functional_edges_df = pd.read_csv(functional_edges_input_filename)
    module_logger.info(f'functional_edges_df.shape = {functional_edges_df.shape}')

    module_logger.info('making network_df using structural_edges_df and functional_edges_df')
    module_logger.info(f"structural edgeweight is {structural_edge_weight}")
    network_df = make_weighted_network_from_structural_and_functional(
        structural_edges_df, functional_edges_df, structural_edge_weight)
    module_logger.info(f'network_df.shape = {network_df.shape}')

    module_logger.info(f'saving network_df to {network_output_filename}')
    network_df.to_csv(network_output_filename, index=False)
