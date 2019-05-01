import pytest

import pandas as pd
import pandas.testing as pd_testing

from src.features.make_structural_network import *
#
#


# @pytest.fixture(scope='module')
# def all_links_test_sample_df():
#     return pd.read_csv('tests/unit/fixtures/all_links_test_sample.csv')

#
# def test_str_array_to_array():
#     assert str_array_to_array("['a','b']") == ['a', 'b']
#
#
# def test_df_str_arrays_to_arrays():
#     assert df_str_arrays_to_arrays(api_links_df, STRUCTURAL_LINK_TYPES) == pd.read]
#


def test_make_structural_network():
    pd_testing.assert_frame_equal(
        make_structural_network(
            pd.read_csv('tests/unit/fixtures/all_links_test_sample.csv')).reset_index(drop=True),
        pd.read_csv('tests/unit/fixtures/structural_network_test_sample.csv'))
