import json

import pandas as pd
import pandas.testing as pd_testing

from src.features.make_structural_network import make_structural_network_and_node_id_mappings


def test_make_structural_network():
    edges_df, content_id_node_id_mapping, node_id_content_id_mapping = make_structural_network_and_node_id_mappings(
            pd.read_csv('tests/unit/fixtures/all_links_test_sample.csv'))

    pd_testing.assert_frame_equal(
        edges_df.reset_index(drop=True),
        pd.read_csv('tests/unit/fixtures/structural_network_test_sample.csv'))

    with open(
            'tests/unit/fixtures/content_id_node_id_mapping.json',
            'r') as file_1:
        assert content_id_node_id_mapping == json.load(file_1)

    # saving an integer-keyed dict down as a json converted the ints to
    # strings, so need to convert them back when comparing
    with open(
            'tests/unit/fixtures/node_id_content_id_mapping.json',
            'r') as file_2:
        assert node_id_content_id_mapping == dict(
            (int(k), v) for k, v in json.load(file_2).items())
