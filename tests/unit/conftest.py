import json

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def structural_edges_fixture():
    return pd.read_csv('tests/unit/fixtures/structural_edges_test_sample.csv')


@pytest.fixture(scope="session")
def content_id_node_id_mapping_fixture():
    with open(
            'tests/unit/fixtures/content_id_node_id_mapping.json', 'r'
    ) as content_id_node_id_mapping_file:
        return json.load(content_id_node_id_mapping_file)


@pytest.fixture(scope="session")
def structural_network_fixture():
    return pd.read_csv(
        'tests/unit/fixtures/structural_network_test_sample.csv',
        dtype={'destination_base_path':object,
               'destination_content_id': object,
               'link_type': object,
               'source_base_path': object,
               'source_content_id': object,
               'source': object,
               'target': object})


@pytest.fixture(scope="session")
def node_id_content_id_mapping_fixture():
    with open(
            'tests/unit/fixtures/node_id_content_id_mapping.json',
            'r') as node_id_content_id_mapping_file:
        return json.load(node_id_content_id_mapping_file)
