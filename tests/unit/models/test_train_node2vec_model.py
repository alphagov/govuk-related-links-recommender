import pytest
import random
import numpy as np
import pandas as pd
import os
from src.utils.epoch_logger import EpochLogger
from gensim.models import Word2Vec
from src.models.train_node2vec_model import N2VModel

# set seed for deterministic tests etc
random.seed(2)
np.random.seed(2)


@pytest.mark.skip(reason="not staying deterministic")
def test_train_node2_vec_model(all_network_fixture):
    # TODO if this needs to be deterministic use the same random walks for training the model (probably)
    n2v = N2VModel()

    n2v.create_graph(all_network_fixture)

    n2v.generate_walks(workers=1)

    n2v.fit_model(workers=1)

    # test we get the same most similar nodes
    print(n2v.model.wv.most_similar(
        "8ccf089c-a512-452f-b027-dee409feb1f3", topn=10))
    assert n2v.model.wv.most_similar(
        "8ccf089c-a512-452f-b027-dee409feb1f3", topn=10) == [
        ('2', 0.9990991353988647), ('501', 0.9988441467285156), ('502', 0.9988089799880981),
        ('500', 0.9987748265266418), ('505', 0.998684287071228), ('504', 0.9986145496368408),
        ('499', 0.9983056783676147), ('1', 0.9978947639465332), ('0', 0.9414731860160828), ('380', 0.6794115304946899)]

    # test an embedding vector
    print(n2v.model.wv['d6204ab9-99fd-4265-a081-5e4c6fdfe11d'])
    np.testing.assert_array_equal(
        n2v.model.wv['d6204ab9-99fd-4265-a081-5e4c6fdfe11d'],
        np.array(
            [-0.88379043, -0.30888686, -0.1813301, -0.067976356, -0.4609355, -0.35824582, -0.08936877, 0.08270162,
             -0.6352391, 0.18851767, -0.14360602, -0.6236442, -0.10684205, 0.16706303, -0.017764626, -0.19982526,
             -0.08983809, 0.23044425, -0.60631204, 0.21698123, -0.05427989, 0.24906242, 0.6751661, -0.6701944,
             -0.08946387, 1.2561842, -0.3632976, -0.33041343, -0.42487434, 1.3301302, -0.1845399, -1.1003605,
             0.23692428, -1.5703396, 0.69791293, -0.14096017, -0.3939386, -0.7056384, 0.20997848, -0.013814987,
             0.3107433, -0.059977658, -0.06617668, 0.20900504, -0.7393868, 0.8214224, -0.10128406, 0.3389172,
             -0.89528805, -0.41821027, 0.3999012, -0.06859911, -0.70715356, -0.45099786, -0.16714878, -0.3983585,
             0.5686123, -0.37777436, -0.6602222, 0.58181953, -1.0959847, -0.09768479, 0.38792232, 0.32195967],
            dtype='float32'))


def test_save_model(weighted_graph_config_fixture):
    """
    Tests model save/load works
    Trains model, saves, loads and checks the model vocab is as trained
    """
    w_config = weighted_graph_config_fixture
    model_dir = 'tests/unit/tmp/'

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

    node_embeddings_file_path = os.path.join(model_dir,
                                             w_config['embeddings_filename'])

    node2vec_model_file_path = os.path.join(model_dir, w_config['model_filename'])

    wm.save_model(node_embeddings_file_path, node2vec_model_file_path)

    trained_model = Word2Vec.load(os.path.join(model_dir, w_config['model_filename']))
    print(set(trained_model.wv.vocab.keys()))
    assert set(map(int, trained_model.wv.vocab.keys())) == vocab


def test_node_sampling(weighted_graph_config_fixture):
    """
    Tests 0 weight transitions are not sampled
    Tests high weight transitions are sampled
    Tests number of transitions == num unique nodes * (walk length - 1) * num walks per node
    Tests nodes in = nodes out
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

    n_nodes = len(set(edges.source_content_id))
    n_transitions = n_nodes * (w_config['walk_length'] - 1) * w_config['num_walks']

    res = np.array([np.array(list(zip(x, x[1:]))).ravel() for x in wm.node2vec.walks])
    walks = np.reshape(res, (n_transitions, 2))

    pairs = pd.DataFrame({'state1': walks[:, 0], 'state2': walks[:, 1]})
    counts = pairs.groupby('state1')['state2'].value_counts().unstack()
    counts = counts.replace(np.nan, 0)
    assert pairs.shape == (n_nodes * (w_config['walk_length'] - 1) * w_config['num_walks'], 2)
    assert counts.iloc[5][4] == 0
    assert counts.iloc[4][5] != 0
    assert len(set(edges['source_content_id']).union(
        set(edges['destination_content_id']))) == len(wm.model.wv.vocab.keys())
