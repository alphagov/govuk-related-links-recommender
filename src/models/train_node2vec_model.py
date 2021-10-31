import os
import logging.config
from multiprocessing import cpu_count
import networkx as nx
from node2vec import Node2Vec

from src.utils.epoch_logger import EpochLogger
from src.utils.miscellaneous import read_config_yaml, safe_getenv
import pandas as pd

logging.config.fileConfig('src/logging.conf')


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

        self.logger.info(f'number of workers is {workers}')

        self.node2vec = Node2Vec(self.graph,
                                 dimensions=dimensions,
                                 walk_length=walk_length,
                                 num_walks=num_walks,
                                 workers=workers)

    def fit_model(self, window=10, min_count=1,
                  batch_words=4, seed=1, workers=None, callbacks=None,
                  iter=5, **kwargs):

        # Try setting workers=1 if you want a deterministic output
        self.logger.info('Fit node2vec model (create embeddings for nodes)')
        if workers is None:
            workers = cpu_count()

        self.logger.info(f'number of workers is {workers}')
        self.logger.info(f'window is {window}')
        self.logger.info(f'batch_words is {batch_words}')
        self.logger.info(f'iter is {iter}')
        # Any keywords acceptable by gensim.Word2Vec can be passed, `dimensions` and `workers` are
        # automatically passed (from the Node2Vec constructor)
        # https://radimrehurek.com/gensim/models/word2vec.html#gensim.models.word2vec.Word2Vec
        self.model = self.node2vec.fit(window=window,
                                       min_count=min_count,
                                       batch_words=batch_words,
                                       seed=seed,
                                       workers=workers,
                                       callbacks=[callbacks],
                                       epochs=iter)

    def save_model(self, embeddings_filepath, model_file_path):
        self.logger.info(f'saving  embeddings to {embeddings_filepath}')
        self.model.wv.save_word2vec_format(embeddings_filepath)

        self.logger.info(f'saving model to {model_file_path}')
        self.model.save(model_file_path)


if __name__ == "__main__":  # our module is being executed as a program

    data_dir = safe_getenv('DATA_DIR')
    preprocessing_config = read_config_yaml("preprocessing-config.yml")
    node2vec_config = read_config_yaml("node2vec-config.yml")

    network_input_filename = os.path.join(data_dir, preprocessing_config['network_filename'])
    model_filename = os.path.join(data_dir, preprocessing_config['model_filename'])
    node_embeddings_filename = os.path.join(data_dir, preprocessing_config['embeddings_filename'])

    module_logger = logging.getLogger('train_node2_vec_model')

    module_logger.info(f'reading in {network_input_filename}')
    edges = pd.read_csv(network_input_filename, dtype={'source_content_id': object, 'destination_content_id': object})

    node2vec_model = N2VModel()

    node2vec_model.create_graph(edges, node2vec_config['weighted_graph'])

    node2vec_model.generate_walks(**node2vec_config)

    node2vec_model.fit_model(**node2vec_config, callbacks=EpochLogger())

    node2vec_model.save_model(node_embeddings_filename, model_filename)
