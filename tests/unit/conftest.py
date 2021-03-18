import json

import pandas as pd
import pytest
import yaml
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
        dtype={'destination_base_path': object,
               'destination_content_id': object,
               'link_type': object,
               'source_base_path': object,
               'source_content_id': object,
               'source': object,
               'target': object})


@pytest.fixture(scope="session")
def all_network_fixture():
    return pd.read_csv(
        'tests/unit/fixtures/network.csv',
        dtype={'source': object,
               'target': object})


@pytest.fixture(scope="session")
def node_id_content_id_mapping_fixture():
    with open(
            'tests/unit/fixtures/node_id_content_id_mapping.json',
            'r') as node_id_content_id_mapping_file:
        return json.load(node_id_content_id_mapping_file)


@pytest.fixture(scope="session")
def functional_edges_fixture():
    return pd.read_csv("tests/unit/fixtures/test_edge_and_weights_20190512.csv").reset_index(
        drop=True)


@pytest.fixture(scope="session")
def weighted_graph_config_fixture():
    with open('tests/unit/fixtures/weighted_config.yml') as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def unweighted_graph_config_fixture():
    with open('tests/unit/fixtures/unweighted_config.yml') as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def weighted_network_fixture():
    return pd.read_csv("tests/unit/fixtures/test_weighted_network.csv",
                       dtype={'weight': 'int64'}).reset_index(
        drop=True)


@pytest.fixture(scope="session")
def page_path_content_id_mapping_fixture():
    with open('tests/unit/fixtures/page_path_content_id_mapping_test_sample.json',
              'r') as page_path_content_id_mapping_test_sample_file:
        return json.load(page_path_content_id_mapping_test_sample_file)


@pytest.fixture(scope="session")
def content_id_base_path_mapping_fixture():
    with open('tests/unit/fixtures/content_id_base_path_mapping.json',
              'r') as content_id_base_path_file:
        return json.load(content_id_base_path_file)
