import json

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def all_links_fixture():
    return pd.read_csv('tests/unit/fixtures/all_links_test_sample.csv')


@pytest.fixture(scope="session")
def content_id_node_id_mapping_fixture():
    with open(
            'tests/unit/fixtures/content_id_node_id_mapping.json', 'r'
    ) as content_id_node_id_mapping_file:
        return json.load(content_id_node_id_mapping_file)


@pytest.fixture(scope="session")
def structural_network_fixture():
    return pd.read_csv(
        'tests/unit/fixtures/structural_network_test_sample.csv')


@pytest.fixture(scope="session")
def node_id_content_id_mapping_fixture():
    with open(
            'tests/unit/fixtures/node_id_content_id_mapping.json',
            'r') as node_id_content_id_mapping_file:
        return dict(
            (int(node_id), content_id) for node_id, content_id in json.load(
                node_id_content_id_mapping_file).items())
