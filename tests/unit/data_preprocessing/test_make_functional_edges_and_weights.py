import pandas.testing as pd_testing
import pandas as pd
import pytest
from src.data_preprocessing.make_functional_edges_and_weights import *
from src.utils.miscellaneous import get_excluded_document_types


@pytest.mark.skip(reason="Don't run up a big big query bill")
def test_return_data_frame(functional_edges_fixture):
    instance = EdgeWeightExtractor(get_excluded_document_types(), "20190512", "20190512")
    pd.set_option('display.max_colwidth', -1)
    merged = instance.df.merge(functional_edges_fixture,
                               on=['source_node', 'destination_node', 'weight'],
                               indicator=True,
                               how='outer')

    print(merged[merged['_merge'] != 'both'])
    print(merged[merged['_merge'] != 'both'].shape[0])
    assert merged[merged['_merge'] != 'both'].shape[0] == 0
