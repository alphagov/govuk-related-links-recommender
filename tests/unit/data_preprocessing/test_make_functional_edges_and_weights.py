import pandas.testing as pd_testing
import pandas as pd
import pytest
from src.data_preprocessing.make_functional_edges_and_weights import *
from src.utils.miscellaneous import get_excluded_document_types


@pytest.fixture
def expected_edge_and_weights():
    return pd.read_csv("tests/unit/fixtures/test_edge_and_weights_20190512.csv").reset_index(
        drop=True)


def test_return_data_frame(expected_edge_and_weights):
    instance = EdgeWeightExtractor(get_excluded_document_types(), "20190512", "20190512")
    pd.set_option('display.max_colwidth', -1)
    merged = instance.df.merge(expected_edge_and_weights,
                               on=['source_node', 'destination_node', 'weight'],
                               indicator=True,
                               how='outer')

    print(merged[merged['_merge'] != 'both'])
    print(merged[merged['_merge'] != 'both'].shape[0])
    assert merged[merged['_merge'] != 'both'].shape[0] == 0
