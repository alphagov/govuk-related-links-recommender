import random
import numpy as np
import pandas as pd
import os
from src.utils.epoch_logger import EpochLogger
from src.models.train_node2vec_model import N2VModel
from gensim.models import Word2Vec

import logging.config


# set seed for deterministic tests etc
random.seed(2)
np.random.seed(2)

logging.config.fileConfig('src/logging.conf')
module_logger = logging.getLogger('test_save_model')


def test_weighted_node2vec(weighted_graph_config_fixture):
    """
    Tests model save/load works
    Trains model, saves, loads and checks the model vocab is as trained
    """
    w_config = weighted_graph_config_fixture

    # trying a hardcoded model directory to see if the test will pass
    # model_dir = os.getenv("MODEL_DIR")
    model_dir = os.path.join(os.getcwd(), "tests/unit/tmp")

    # Node 5 to node 4 has zero weight (zero transition probability)
    # Node 4 to node 5 has ten weight (high transition probability)

    source_ids = [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5]
    destination_ids = [5, 1, 0, 3, 4, 1, 2, 1, 3, 5, 3, 4]
    weights = [1, 2, 3, 4, 1, 2, 3, 4, 1, 10, 5, 0]
    vocab = set(source_ids).union(set(destination_ids))

    edges = pd.DataFrame({'source_content_id': source_ids,
                          'destination_content_id': destination_ids,
                          'weight': weights}
                         )


    wm = N2VModel()

    wm.create_graph(edges, w_config['weighted_graph'])

    wm.generate_walks(**w_config)

    wm.fit_model(**w_config, callbacks=EpochLogger())

    module_logger.info(f'model_dir is {model_dir}')

    node_embeddings_file_path = os.path.join(model_dir,
                                             w_config['embeddings_filename'])

    node2vec_model_file_path = os.path.join(model_dir, w_config['model_filename'])

    module_logger.info(f'node2vec_model_file_path is {node2vec_model_file_path}')

    wm.save_model(node_embeddings_file_path, node2vec_model_file_path)

    trained_model = Word2Vec.load(os.path.join(model_dir, w_config['model_filename']))

    module_logger.info(f'vocab is {set(trained_model.wv.vocab.keys())}')
    module_logger.info(f'expected vocab is {vocab}')

    print(set(trained_model.wv.vocab.keys()))
    assert set(map(int, trained_model.wv.vocab.keys())) == vocab
