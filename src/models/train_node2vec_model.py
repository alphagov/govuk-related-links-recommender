import json
import os
import logging.config
from multiprocessing import cpu_count

import networkx as nx
from node2vec import Node2Vec
import pandas as pd


logging.config.fileConfig('src/logging.conf')


def create_graph(edges_df, node_id_content_id_mapping):
    logger = logging.getLogger('train_node2_vec_model.create_graph')
    logger.info('creating graph from edges_df')
    graph = nx.convert_matrix.from_pandas_edgelist(
        edges_df, source='source', target='target'
        # when we have functional network too, we can add weight
        # , edge_attr='weight'
    )
    return graph


def train_node2_vec_model(edges_df, node_id_content_id_mapping,
                          workers=None):
    """
    Train a node2vec model using a DataFrame of edges (source and target node_ids)
    and a mapping of the node_ids (used in the DataFrame) to GOV.UK content_ids
    :param edges_df: pandas DataFrame with source and target columns (containing node_ids)
    :param node_id_content_id_mapping: Python dictionary {node_id: content_id}
    :param workers: (optional, default=number of CPUs) number of workers to use for the node2vec random walks and fitting
    :return: a node2vec model
    """
    logger = logging.getLogger('train_node2_vec_model.train_node2_vec_model')

    if workers is None:
        workers = cpu_count()

    logger.info(f'number of workers is {workers}')

    graph = create_graph(edges_df, node_id_content_id_mapping)

    logger.info('Precomputing probabilities and generating walks')
    # TODO: search this parameter space systematically and change node2vec parameters
    node2vec = Node2Vec(graph, dimensions=64, walk_length=30, num_walks=300,
                        workers=workers)

    logger.info('Fit node2vec model (create embeddings for nodes)')
    # Any keywords acceptable by gensim.Word2Vec can be passed, `dimensions` and `workers` are
    # automatically passed (from the Node2Vec constructor)
    # https://radimrehurek.com/gensim/models/word2vec.html#gensim.models.word2vec.Word2Vec
    # TODO: search this parameter space systematically and change node2vec parameters
    model = node2vec.fit(window=10, min_count=1, batch_words=4, seed=1,
                         workers=workers)
    return model


if __name__ == "__main__":  # our module is being executed as a program
    data_dir = os.getenv("DATA_DIR")
    model_dir = os.getenv("MODEL_DIR")
    module_logger = logging.getLogger('train_node2_vec_model')

    module_logger.info(f'reading in structural_network.csv and node_id_content_id_mapping.json')
    edges = pd.read_csv(os.path.join(
        data_dir, 'tmp', 'structural_network.csv'),
        dtype={'destination_base_path':object,
               'destination_content_id': object,
               'link_type': object,
               'source_base_path': object,
               'source_content_id': object,
               'source': object,
               'target': object})

    # TODO: make this "read json file and convert the keys to ints" step into a function, for use
    #  here and tes_make_structural_netork
    with open(
            os.path.join(data_dir, 'tmp', 'node_id_content_id_mapping.json'),
            'r') as node_id_content_id_mapping_file:
        node_id_content_id_mapping_dict = json.load(
                node_id_content_id_mapping_file)

    node2vec_model = train_node2_vec_model(edges,
                                           node_id_content_id_mapping_dict)

    # TODO:think about a function that gets the filepath for the thing and does that saving.
    #  Depends how often we do that whether it's needed. Not for MVP
    node_embeddings_file_path = os.path.join(model_dir,
                                             "n2v_node_embeddings")
    module_logger.info(f'saving node embeddings to {node_embeddings_file_path}')
    node2vec_model.wv.save_word2vec_format(node_embeddings_file_path)

    node2vec_model_file_path = os.path.join(model_dir, "n2v.model")
    module_logger.info(f'saving model to {node2vec_model_file_path}')
    node2vec_model.save(node2vec_model_file_path)
    # should we test saving and loading models and embeddings?
    # TODO: might be useful to have some utility class somewhere that deals with read/write
    #  and is tested there. Probably not needed for MVP.
