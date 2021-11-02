import os
import logging.config
from multiprocessing import cpu_count
from fastnode2vec import Graph, Node2Vec

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

    def generate_walks(self, dimensions=64, walk_length=10, num_walks=300, workers=None, **kwargs):

        self.logger.info('Precomputing probabilities and generating walks')

        if workers is None:
            workers = cpu_count()

        self.logger.info(f'number of workers is {workers}')

        self.node2vec = Node2Vec(self.graph,
                                 dim=dimensions,
                                 context=10,
                                 walk_length=walk_length,
                                 batch_walks=num_walks,
                                 workers=workers)

    def fit_model(self,
                  seed=1, workers=None, callbacks=None,
                  epochs=5, **kwargs):

        # Try setting workers=1 if you want a deterministic output
        self.logger.info('Fit node2vec model (create embeddings for nodes)')
        if workers is None:
            workers = cpu_count()

        self.logger.info(f'number of workers is {workers}')
        self.logger.info(f'epochs is {epochs}')
        # Any keywords acceptable by gensim.Word2Vec can be passed, `dimensions` and `workers` are
        # automatically passed (from the Node2Vec constructor)
        # https://radimrehurek.com/gensim/models/word2vec.html#gensim.models.word2vec.Word2Vec
        self.model = self.node2vec.train(callbacks=[callbacks],
                                         epochs=epochs)

    def save_model(self, embeddings_filepath, model_file_path):
        self.logger.info(f'saving  embeddings to {embeddings_filepath}')
        self.node2vec.wv.save_word2vec_format(embeddings_filepath)

        self.logger.info(f'saving model to {model_file_path}')
        self.node2vec.save(model_file_path)


if __name__ == "__main__":  # our module is being executed as a program

    data_dir = safe_getenv('DATA_DIR')
    preprocessing_config = read_config_yaml("preprocessing-config.yml")
    node2vec_config = read_config_yaml("node2vec-config.yml")

    network_input_filename = os.path.join(data_dir, preprocessing_config['network_filename'])
    model_filename = os.path.join(data_dir, preprocessing_config['model_filename'])
    node_embeddings_filename = os.path.join(data_dir, preprocessing_config['embeddings_filename'])

    module_logger = logging.getLogger('train_node2_vec_model')

    module_logger.info(f'reading in {network_input_filename}')
    edges = pd.read_csv(network_input_filename,
                        dtype={'source_content_id': str, 'destination_content_id': str, 'weight': 'int32'})

    node2vec_model = N2VModel()

    edges_records = edges.to_records(index=False)
    node2vec_model.graph = Graph(list(edges_records), directed=False, weighted=True)

    node2vec_model.generate_walks(**node2vec_config)

    node2vec_model.fit_model(**node2vec_config, callbacks=EpochLogger())

    node2vec_model.save_model(node_embeddings_filename, model_filename)
