import json

import pandas as pd
import pandas.testing as pd_testing

from src.features.make_structural_network import make_structural_network_and_node_id_mappings


def test_make_structural_network(
        all_links_fixture, content_id_node_id_mapping_fixture,
        structural_network_fixture, node_id_content_id_mapping_fixture):
    edges_df, content_id_node_id_mapping, node_id_content_id_mapping = make_structural_network_and_node_id_mappings(
        all_links_fixture)

    pd_testing.assert_frame_equal(
        edges_df.reset_index(drop=True),
        structural_network_fixture)

    assert content_id_node_id_mapping == content_id_node_id_mapping_fixture

    # saving an integer-keyed dict down as a json converted the ints to
    # strings, so need to convert them back when comparing
    assert node_id_content_id_mapping == node_id_content_id_mapping_fixture
