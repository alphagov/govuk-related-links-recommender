import logging.config
import os
import pandas as pd
from src.utils.miscellaneous import safe_getenv

# link types we want to use for our structural network
logging.config.fileConfig('src/logging.conf')


def make_network_from_structural_and_functional(structural_edges, functional_edges):
    """
    Combine structural and functional dataframes to get a deduplicated dataframe of edges
    :param structural_edges: pandas DataFrame including columns ['source_content_id', 'destination_content_id']
    :param functional_edges: pandas DataFrame including columns ['source_content_id', 'destination_content_id']
    :return: pandas DataFrame with columns ['source_content_id', 'destination_content_id']
    """
    all_edges = pd.concat([structural_edges, functional_edges],
                          ignore_index=True, sort=True)
    network_edges = all_edges[
        ['source_content_id', 'destination_content_id', 'weight']].drop_duplicates().reset_index(drop=True)

    return network_edges


if __name__ == "__main__":  # our module is being executed as a program

    data_dir = safe_getenv('DATA_DIR')
    functional_edges_input_filename = os.path.join(data_dir, 'functional_edges.csv')
    structural_edges_input_filename = os.path.join(data_dir, 'structural_edges.csv')
    network_output_filename = os.path.join(data_dir, 'network.csv')

    module_logger = logging.getLogger('make_structural_network')

    module_logger.info(f'reading {structural_edges_input_filename}')
    structural_edges_df = pd.read_csv(structural_edges_input_filename)
    module_logger.info(f'structural_edges_df.shape = {structural_edges_df.shape}')

    module_logger.info(f'reading {functional_edges_input_filename}')
    functional_edges_df = pd.read_csv(functional_edges_input_filename)
    module_logger.info(f'functional_edges_df.shape = {functional_edges_df.shape}')

    module_logger.info('making network_df using structural_edges_df and functional_edges_df')
    network_df = make_network_from_structural_and_functional(structural_edges_df, functional_edges_df)
    module_logger.info(f'network_df.shape = {network_df.shape}')

    module_logger.info(f'saving network_df to {network_output_filename}')
    network_df.to_csv(network_output_filename, index=False)
