import json

import pandas as pd
import pandas.testing as pd_testing

from src.features.make_network import make_network_and_node_id_mappings, combine_structural_functional_edges


def test_make_network(
        all_edges_fixture, content_id_node_id_mapping_fixture,
        all_network_fixture, node_id_content_id_mapping_fixture):
    network_df, content_id_node_id_mapping, node_id_content_id_mapping = make_network_and_node_id_mappings(
        all_edges_fixture)

    pd_testing.assert_frame_equal(
        network_df.reset_index(drop=True),
        all_network_fixture)

    assert content_id_node_id_mapping == content_id_node_id_mapping_fixture

    # saving an integer-keyed dict down as a json converted the ints to
    # strings, so need to convert them back when comparing
    assert node_id_content_id_mapping == node_id_content_id_mapping_fixture


def test_combine_structural_functional_edges(
        structural_edges_fixture, functional_edges_fixture, all_edges_fixture):
    pd_testing.assert_frame_equal(combine_structural_functional_edges(
        structural_edges_fixture, functional_edges_fixture), all_edges_fixture)
