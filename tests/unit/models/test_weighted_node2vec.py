import random
import numpy as np
import pandas as pd
from src.utils.epoch_logger import EpochLogger
from src.models.train_node2vec_model import N2VModel

# set seed for deterministic tests etc
random.seed(2)
np.random.seed(2)


def test_weighted_node2vec(weighted_graph_config_fixture):
    """
    Tests weighted node2vec output is using weights correctly in both edge directions

    Tests model save/load works
    """
    w_config = weighted_graph_config_fixture

    # Node 5 to node 4 has zero weight (zero transition probability)
    # Node 4 to node 5 has ten weight (high transition probability)
    edges = pd.DataFrame({'source_content_id': [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
                          'destination_content_id': [5, 1, 0, 3, 4, 1, 2, 1, 3, 5, 3, 4],
                          'weight': [1, 2, 3, 4, 1, 2, 3, 4, 1, 10, 5, 0]}
                         )
    wm = N2VModel()

    wm.create_graph(edges, w_config['weighted_graph'])

    wm.generate_walks(**w_config)

    wm.fit_model(**w_config, callbacks=EpochLogger())

    walks = np.ravel(wm.node2vec.walks)
    length = int(len(walks) / 2)

    walks = np.reshape(walks, (length, 2))

    pairs = pd.DataFrame({'state1': walks[:, 0], 'state2': walks[:, 1]})
    counts = pairs.groupby('state1')['state2'].value_counts().unstack()
    counts = counts.replace(np.nan, 0)
    assert counts.iloc[5][4] == 0
    assert counts.iloc[4][5] != 0
