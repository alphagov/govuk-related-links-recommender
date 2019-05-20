import pandas.testing as pd_testing

from src.features.make_network import make_network_from_structural_and_functional


def test_make_network(
        structural_edges_fixture, functional_edges_fixture, all_network_fixture):

    pd_testing.assert_frame_equal(
        make_network_from_structural_and_functional(
            structural_edges_fixture, functional_edges_fixture
        ).reset_index(drop=True),
        all_network_fixture)
