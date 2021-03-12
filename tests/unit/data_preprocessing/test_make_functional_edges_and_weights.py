import pandas as pd
import pytest
from src.data_preprocessing.make_functional_edges_and_weights import EdgeWeightExtractor
from src.utils.miscellaneous import read_config_yaml


@pytest.mark.skip(reason="Concourse doesnt have big query creds")
def test_return_data_frame():
    """Tests that Edgeweight Extractor instantiates and runs query and result is unique set of edges and counts"""
    exclusions = read_config_yaml("document_types_excluded_from_the_topic_taxonomy.yml")['document_types']
    instance = EdgeWeightExtractor('src/data_preprocessing/intra_day_content_id_edge_weights.sql',
                                   exclusions,
                                   )

    instance.create_df()
    pd.set_option('display.max_colwidth', -1)

    instance.df['unique_edges'] = instance.df['source_content_id'] + instance.df['destination_content_id']
    assert(instance.df.shape[0] > 1)
    assert(len(set(instance.df['unique_edges'])) == instance.df.shape[0])
