# import random
import json
import pandas as pd

from src.models.train_node2vec_model import *

# set seed for deterministic tests etc
# random.seed(123)


def test_train_node2_vec_model():
    structural_network = pd.read_csv(
        'tests/unit/fixtures/structural_network_test_sample.csv')
    with open('tests/unit/fixtures/node_id_content_id_mapping.json', 'r'
              ) as file_2:
        node_id_content_id_mapping = dict(
            (int(k), v) for k, v in json.load(file_2).items())

    model = train_node2_vec_model(structural_network,
                                  node_id_content_id_mapping)

    print(model.wv.most_similar(
        "503", topn=10))
    assert model.wv.most_similar(
        "503", topn=10) == [('503', 0.9944461584091187)]

