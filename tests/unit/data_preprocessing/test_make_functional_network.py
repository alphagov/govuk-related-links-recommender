from src.data_preprocessing.make_functional_network import *
import pandas as pd
import pandas.testing as pd_testing
import pickle
import pytest
import ast

from tests.unit.data_preprocessing.big_query_test_data_extractor import BigQueryTestDataExtractor

@pytest.fixture
def sequence_split_to_list():
    with open('tests/unit/fixtures/list_column.pkl', 'rb') as fp:
        sequence_split_to_list = pickle.load(fp)
    return sequence_split_to_list


@pytest.fixture
def test_raw_bq_output():
    return pd.read_csv('tests/unit/fixtures/test_raw_bq_output.csv')


@pytest.fixture
def edge_counter():
    with open('tests/unit/fixtures/test_edge_counter.pkl', 'rb') as fp:
        edge_counter = pickle.load(fp)
    return edge_counter


@pytest.fixture
def node_pair_in_df():
    df = pd.read_csv('tests/unit/fixtures/test_node_pairs.csv')
    # df['node_pairs'] = df['node_pairs'].apply(ast.literal_eval)
    return df


@pytest.fixture
def computed_occurrences():
    frequencies = pd.read_csv('tests/unit/fixtures/computed_occurrences.csv', header=None).iloc[:, 0]
    frequencies.name = 'Occurrences'
    return frequencies


@pytest.fixture
def unique_sequences_weights():
    return pd.read_csv('tests/unit/fixtures/unique_sequences_weights.csv')

@pytest.fixture
def node_id_mapper_small():
    return {'76698ffe-70ab-4fda-be0d-755234f6d340': 0,
                      'f9015c31-61c2-4504-8eb0-242cd75aee19': 1,
                      '1d5b7656-fdc1-4802-9974-39ac538e5a15': 2,
                      'e7bdb8d9-2c5a-488a-85ef-bb4515091bf4': 3}
@pytest.fixture
def node_id_mapper_big():
    with open('/tests/unit/fixtures/test_node_dict.pkl',
              'rb') as handle:
        node_ids = pickle.load(handle)
    return node_ids

@pytest.fixture
def edge_counter_sample():
    return Counter({('76698ffe-70ab-4fda-be0d-755234f6d340',
                      'f9015c31-61c2-4504-8eb0-242cd75aee19'): 2,
                     ('f9015c31-61c2-4504-8eb0-242cd75aee19',
                      'f9015c31-61c2-4504-8eb0-242cd75aee19'): 16,
                     ('1d5b7656-fdc1-4802-9974-39ac538e5a15',
                      '1d5b7656-fdc1-4802-9974-39ac538e5a15'): 37,
                     ('e7bdb8d9-2c5a-488a-85ef-bb4515091bf4',
                      'e7bdb8d9-2c5a-488a-85ef-bb4515091bf4'): 16})



def test_get_list_col_from_sequence(test_raw_bq_output, sequence_split_to_list):
    data_extractor = BigQueryTestDataExtractor()
    instance = FunctionalNetwork(data_extractor)

    assert instance.get_list_col_from_sequence(test_raw_bq_output) == sequence_split_to_list


def test_get_node_pairs_from_sequence_list(sequence_split_to_list):
    data_extractor = BigQueryTestDataExtractor()
    instance = FunctionalNetwork(data_extractor)

    assert instance.get_node_pairs_from_sequence_list(
        sequence_split_to_list[0]) == [['76698ffe-70ab-4fda-be0d-755234f6d340',
                                        'f9015c31-61c2-4504-8eb0-242cd75aee19'],
                                       ['f9015c31-61c2-4504-8eb0-242cd75aee19',
                                        'f9015c31-61c2-4504-8eb0-242cd75aee19'],
                                       ['f9015c31-61c2-4504-8eb0-242cd75aee19',
                                        'f9015c31-61c2-4504-8eb0-242cd75aee19']]

    input_df = pd.DataFrame({'content_id_list': [['76698ffe-70ab-4fda-be0d-755234f6d340',
                                            'f9015c31-61c2-4504-8eb0-242cd75aee19',
                                            'f9015c31-61c2-4504-8eb0-242cd75aee19',
                                            'f9015c31-61c2-4504-8eb0-242cd75aee19'],
                                           ['1d5b7656-fdc1-4802-9974-39ac538e5a15',
                                            '1d5b7656-fdc1-4802-9974-39ac538e5a15'],
                                           ['e7bdb8d9-2c5a-488a-85ef-bb4515091bf4',
                                            'e7bdb8d9-2c5a-488a-85ef-bb4515091bf4',
                                            'e7bdb8d9-2c5a-488a-85ef-bb4515091bf4',
                                            '0b0ff1cc-1532-4cd3-836b-0b7ab0359a4b',
                                            '0b0ff1cc-1532-4cd3-836b-0b7ab0359a4b']
                                           ]
                       })

    result_df = pd.Series([[['76698ffe-70ab-4fda-be0d-755234f6d340', 'f9015c31-61c2-4504-8eb0-242cd75aee19'],
                 ['f9015c31-61c2-4504-8eb0-242cd75aee19', 'f9015c31-61c2-4504-8eb0-242cd75aee19'],
                 ['f9015c31-61c2-4504-8eb0-242cd75aee19', 'f9015c31-61c2-4504-8eb0-242cd75aee19']],
                           [['1d5b7656-fdc1-4802-9974-39ac538e5a15', '1d5b7656-fdc1-4802-9974-39ac538e5a15']],
                           [['e7bdb8d9-2c5a-488a-85ef-bb4515091bf4', 'e7bdb8d9-2c5a-488a-85ef-bb4515091bf4'],
                            ['e7bdb8d9-2c5a-488a-85ef-bb4515091bf4', 'e7bdb8d9-2c5a-488a-85ef-bb4515091bf4'],
                            ['e7bdb8d9-2c5a-488a-85ef-bb4515091bf4', '0b0ff1cc-1532-4cd3-836b-0b7ab0359a4b'],
                            ['0b0ff1cc-1532-4cd3-836b-0b7ab0359a4b', '0b0ff1cc-1532-4cd3-836b-0b7ab0359a4b']]])

    assert pd.Series(
        input_df['content_id_list'].map(instance.get_node_pairs_from_sequence_list)).to_string() == result_df.to_string()


def test_compute_occurrences(node_pair_in_df, computed_occurrences):
    data_extractor = BigQueryTestDataExtractor()
    print(data_extractor)
    instance = FunctionalNetwork(data_extractor)

    pd_testing.assert_series_equal(instance.compute_occurrences(node_pair_in_df),
                                   computed_occurrences)


def test_get_unique_sequences_and_weights(node_pair_in_df, unique_sequences_weights):
    data_extractor = BigQueryTestDataExtractor()
    instance = FunctionalNetwork(data_extractor)

    pd_testing.assert_frame_equal(instance.get_unique_sequences_and_weights(node_pair_in_df), unique_sequences_weights)


def test_get_edges_and_weights(node_pair_in_df, edge_counter):
    data_extractor = BigQueryTestDataExtractor()
    instance = FunctionalNetwork(data_extractor)

    df = node_pair_in_df
    df['node_pairs'] = df['node_pairs'].apply(ast.literal_eval)

    assert instance.get_edges_and_weights(df) == edge_counter

def test_create_node_id_mapper(node_id_mapper_small, edge_counter_sample):
    data_extractor = BigQueryTestDataExtractor()
    instance = FunctionalNetwork(data_extractor)

    assert instance.create_node_id_mapper(edge_counter_sample) == node_id_mapper_small


def test_node_writer(node_id_mapper_small):
    data_extractor = BigQueryTestDataExtractor()
    instance = FunctionalNetwork(data_extractor)

    instance.node_writer("tests/unit/fixtures/node_id_check.csv.gz", "node\tnode_id\n", node_id_mapper_small, None)

    pd_testing.assert_frame_equal(
        pd.read_csv("tests/unit/fixtures/node_id_check.csv.gz", compression='gzip'),
        pd.read_csv("tests/unit/fixtures/node_id_test.csv.gz", compression='gzip'))


def test_edge_writer(node_id_mapper_small, edge_counter_sample):
    data_extractor = BigQueryTestDataExtractor()
    instance = FunctionalNetwork(data_extractor)

    header = "Source_node\tSource_id\tDestination_node\tDestination_id\tWeight\n"

    instance.edge_writer("tests/unit/fixtures/edge_check.csv.gz", header, edge_counter_sample, node_id_mapper_small, None)

    pd_testing.assert_frame_equal(
        pd.read_csv("tests/unit/fixtures/edge_check.csv.gz", compression='gzip'),
        pd.read_csv("tests/unit/fixtures/edge_test.csv.gz", compression='gzip'))

#
def test_create_network(node_id_mapper_big, edge_counter):
    data_extractor = BigQueryTestDataExtractor()
    network_factory = FunctionalNetwork(data_extractor)
    edges, node_ids = network_factory.create_network()
    assert edges == edge_counter
    assert node_ids == node_id_mapper_big









































