from get_bq_data import retrieve_data_from_big_query
from get_content_store_data import reshape_df_explode_list_column
from collections import Counter
import logging.config
import pandas as pd
import os

DATA_DIR = os.getenv("DATA_DIR")
logging.config.fileConfig('src/logging.conf')
logger = logging.getLogger('get_content_store_data')

def main():
    df = retrieve_data_from_big_query()
    df['CIDSequence_list'] = get_list_col_from_sequence(df)
    df['node_pair'] = df['CIDSequence_list'].map(get_list_of_pairs_in_sequence_list)
    compute_occurrences(df, page_sequence='CIDSequence', occurrences='CIDOccurrences')
    df = reshape_df_explode_list_column(df[['CIDSequence', 'CIDOccurrences', 'node_pair']], 'node_pair')
    df.drop_duplicates(subset='CIDSequence', inplace=True)
    df['weight'] = compute_weights(df)
    df.drop_duplicates('node_pair', inplace=True)
    df = split_pair_string_into_two_cols(df)
    node_ids = get_node_ids(df)
    df = map_content_id_to_node_id(df, node_ids)
    node_ids.to_csv(os.path.join(DATADIR, 'tmp', 'functional_nodes.csv.gz'), compression='gzip', index=False)
    df.to_csv(os.path.join(DATADIR, 'tmp', 'functional_edges.csv.gz'), compression='gzip', index=False)


def split_pair_string_into_two_cols(df):
    df[['source_content_id', 'destination_content_id']] = df['pairs'].str.split(">>>", n=1, expand=True)
    return df


def get_list_col_from_sequence(df, sequence_variable='CIDSequence'):
    return [sequence.split(">>") for sequence in df[sequence_variable].values]


def get_list_of_pairs_in_sequence_list(sequence_list):
    """
    Build node pairs (edges) from a list of page hits
    :param page_list: list of page hits
    :return: list of all possible node pairs
    """
    return [content_item + '>>>' + sequence_list[i + 1] for i, content_item in enumerate(sequence_list) if
            i < len(sequence_list) - 1]


# TODO this should be done in the SQL query. Need to check the Occurrences between UNTESTED QUERY (no PageSequence) and QUERY.
def compute_occurrences(user_journey_df, page_sequence, occurrences):
    logging.debug("Computing specialized occurrences \"{}\" based on  \"{}\"...".format(occurrences, page_sequence))
    user_journey_df[occurrences] = user_journey_df.groupby(page_sequence)['Occurrences'].transform(
        'sum')


#
def compute_weights(df):
    """
    Creates a column for each edge pair which counts the total number of Occurrences of the pair within all sequences
    Warning, this needs to be done after exploding the DataFrame and removing duplicate CIDSequences, otherwise it will over count.
    :param df: pandas DataFrame containing an 'node_pair' column created using get_list_of_pairs_in_sequence_list
    :return: pandas Series containing a weights column counting the number of times each edge was used
    """
    return df.groupby('node_pair')['CIDOccurrences'].transform(
        'sum')


def get_node_ids(df):
    return pd.Series(
        list(set(df['source_content_id'].unique()).union(set(df['destination_content_id'].unique())))).reset_index(
        name="content_id")

def map_content_id_to_node_id(df, node_ids_df):
    df = pd.merge(df, node_ids_df, left_on="source_content_id", right_on="content_id", how="left")
    df.rename(columns={'index': 'source'}, inplace=True)
    df.drop(columns='content_id', inplace=True)
    df = pd.merge(df, node_ids_df, left_on="destination_content_id", right_on="content_id", how="left")
    df.rename(columns={'index': 'target'}, inplace=True)
    return df.drop(columns=['CIDOccurrences', 'CIDSequence', 'content_id', 'node_pair'], inplace=True)


if __name__ == "__main__":
    main()
