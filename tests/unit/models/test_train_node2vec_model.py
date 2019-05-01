# import random
import pandas as pd

from src.models.train_node2vec_model import *

# set seed for deterministic tests etc
# random.seed(123)


def test_train_node2_vec_model():
    structural_network = pd.read_csv(
        'tests/unit/fixtures/structural_network_test_sample.csv')
    model = train_node2_vec_model(
        structural_network)
    print(model.wv.most_similar(
        "503", topn=10))
    assert model.wv.most_similar(
        "503", topn=10) == [('503', 0.9944461584091187)]

