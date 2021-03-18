import pandas as pd

from src.features.make_weighted_network import make_weighted_network_from_structural_and_functional


def test_make_weighted_network(
        structural_edges_fixture, functional_edges_fixture, weighted_network_fixture):
    """
    Tests:
    1) No edges from structural and functional network are lost in merge
    2) Edges are unique
    3) Every edge has a weight
    """
    weighted_network = make_weighted_network_from_structural_and_functional(
        structural_edges_fixture, functional_edges_fixture
    ).reset_index(drop=True)

    structural_edges_fixture['edges'] = structural_edges_fixture['source_content_id'] + \
        structural_edges_fixture['destination_content_id']
    functional_edges_fixture['edges'] = functional_edges_fixture['source_content_id'] + \
        functional_edges_fixture['destination_content_id']

    z = set(structural_edges_fixture['edges']).union(set(functional_edges_fixture['edges']))

    y = set(weighted_network['source_content_id'] + weighted_network['destination_content_id'])

    assert z == y

    assert len(y) == weighted_network.shape[0]

    assert all(pd.notna(weighted_network['weight']))
