import os
import logging.config
from multiprocessing import cpu_count

import networkx as nx
from node2vec import Node2Vec

from src.utils.epoch_logger import EpochLogger
from src.utils.miscellaneous import read_config_yaml
import pandas as pd


class N2VModel:
    def __init__(self, node2vec=None, graph=None, model=None):
        self.graph = graph
        self.model = model
        self.node2vec = node2vec
        self.logger = logging.getLogger('train_node2_vec_model')

    def create_graph(self, edges_df, weighted=False):
        """
        Creates a networkx graph from edge list dataframe

            Parameters:
                edges_df (Dataframe): List of of [weighted] edges
                weighted (bool): If true, generated the weighted matrix usinng 'weight' col in edges_df

            Returns:

        """
        if not weighted:
            self.logger.info('creating unweighted graph from edges_df')
            self.graph = nx.from_pandas_edgelist(edges_df, source='source_content_id',
                                                 target='destination_content_id',
                                                 create_using=nx.DiGraph())
        else:
            self.logger.info('creating weighted graph from edges_df')
            self.graph = nx.from_pandas_edgelist(edges_df, source='source_content_id',
                                                 target='destination_content_id',
                                                 edge_attr='weight',
                                                 create_using=nx.DiGraph())

    def generate_walks(self, dimensions=64, walk_length=10, num_walks=300, workers=None, **kwargs):

        self.logger.info('Precomputing probabilities and generating walks')
        if workers is None:
            workers = cpu_count()
        else:
            workers = workers

        self.logger.info(f'number of workers is {workers}')

        self.node2vec = Node2Vec(self.graph,
                                 dimensions=dimensions,
                                 walk_length=walk_length,
                                 num_walks=num_walks,
                                 workers=workers)

    def fit_model(self, window=10, min_count=1,
                  batch_words=4, seed=1, workers=None, callbacks=None,
                  epochs=5, **kwargs):
        self.logger.info('Fit node2vec model (create embeddings for nodes)')
        if workers is None:
            workers = cpu_count()
        else:
            workers = workers

        self.logger.info(f'number of workers is {workers}')
        self.logger.info(f'window is {window}')
        self.logger.info(f'batch_words is {batch_words}')
        self.logger.info(f'epochs is {epochs}')
        # Any keywords acceptable by gensim.Word2Vec can be passed, `dimensions` and `workers` are
        # automatically passed (from the Node2Vec constructor)
        # https://radimrehurek.com/gensim/models/word2vec.html#gensim.models.word2vec.Word2Vec
        self.model = self.node2vec.fit(window=window,
                                       min_count=min_count,
                                       batch_words=batch_words,
                                       seed=seed,
                                       workers=workers,
                                       callbacks=[callbacks],
                                       epochs=epochs)

    def save_model(self, embeddings_filepath, model_file_path):
        module_logger.info(f'saving  embeddings to {embeddings_filepath}')
        self.model.wv.save_word2vec_format(embeddings_filepath)

        module_logger.info(f'saving model to  to {model_file_path}')
        self.model.save(model_file_path)


if __name__ == "__main__":  # our module is being executed as a program
    data_dir = os.getenv("DATA_DIR")
    model_dir = os.getenv("MODEL_DIR")
    logging.config.fileConfig('src/logging.conf')
    module_logger = logging.getLogger('train_node2_vec_model')

    prepro_cfg = read_config_yaml(
        "preprocessing-config.yml")

    node2vec_config = read_config_yaml(
        "node2vec-config.yml")

    module_logger.info(f'reading in all_edges.csv and node_id_content_id_mapping.json')
    edges = pd.read_csv(
        os.path.join(data_dir, 'tmp', prepro_cfg['network_filename']),
        dtype={'source_content_id': object,
               'destination_content_id': object}
    )

    node2vec_model = N2VModel()

    node2vec_model.create_graph(edges, node2vec_config['weighted_graph'])

    node2vec_model.generate_walks(**node2vec_config)

    node2vec_model.fit_model(**node2vec_config, callbacks=EpochLogger())

    node_embeddings_file_path = os.path.join(model_dir,
                                             node2vec_config['embeddings_filename'])

    node2vec_model.save(node_embeddings_file_path)

    node2vec_model_file_path = os.path.join(model_dir, node2vec_config['model_filename'])

    node2vec_model.save(node2vec_model_file_path)
