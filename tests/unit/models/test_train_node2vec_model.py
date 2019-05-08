import random
import json
import pandas as pd
import numpy as np

from src.models.train_node2vec_model import train_node2_vec_model

# set seed for deterministic tests etc
random.seed(1)
np.random.seed(1)


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
        "503", topn=10) == [('2', 0.9990638494491577), ('505', 0.9989425539970398), ('501', 0.9985865354537964), ('502', 0.9985626935958862), ('500', 0.99852454662323), ('499', 0.9983034133911133), ('504', 0.998264729976654), ('1', 0.9980345368385315), ('0', 0.943044126033783), ('537', 0.6832790374755859)]
