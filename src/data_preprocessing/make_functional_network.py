import gzip
import logging.config
import os
from collections import Counter

from src.data_preprocessing.big_query_data_extractor import BigQueryDataExtractor


class FunctionalNetwork:
    # TODO work out how to use os.getenv in class variable
    EDGE_OUTPUT_FILE = os.path.join("data", "tmp", "functional_edges_dict.csv.gz")
    NODE_OUTPUT_FILE = os.path.join("data", "tmp", "functional_nodes_dict.csv.gz")

    logging.config.fileConfig('src/logging.conf')
    logger = logging.getLogger('get_content_store_data')

    def __init__(self, data_extractor):

        self.data_extractor = data_extractor

    def create_network(self):
        """
        Extract last 3 weeks data from Big Query and construct weighted edge_lists (node pairs) and node_ids.
        Write edges and nodes to compressed csv files
        """
        df = self.data_extractor.retrieve_data_from_big_query()
        df['content_id_list'] = self.get_list_col_from_sequence(df)
        df['node_pairs'] = df['content_id_list'].map(self.get_node_pairs_from_sequence_list)

        edges = self.get_edges_and_weights(df)
        node_ids = self.create_node_id_mapper(edges)

        self.logger.info(list(node_ids.items())[0:10])

        # TODO move these defaults into the writer args
        default_edge_header = "source_node\tsource_id\tdestination_node\tdestination_id\tweight\n"
        default_node_header = "node\tnode_id\n"
        node_attributes = None  # not used in Node2vec algorithm

        self.logger.info("Number of nodes: {} Number of edges: {}".format(len(node_ids), len(edges)))
        self.logger.info("Writing edge list to file...")

        self.edge_writer(self.EDGE_OUTPUT_FILE, default_edge_header, edges,
                         node_ids,
                         node_attributes)
        self.node_writer(self.NODE_OUTPUT_FILE, default_node_header,
                         node_ids,
                         node_attributes)

        return edges, node_ids

    def get_list_col_from_sequence(self, df, sequence_variable='CIDSequence'):
        """
        Convert the sequence (either of pages or content IDs), which is separated by ">>" into a python list
        :param df: pandas DataFrame containing a sequence column
        :param sequence_variable: name of sequence column (default ='CIDSequence') with content_ids separated by >>
        :return: list where each element is a list of the pages/content_ids visited
        """
        return [sequence.split(">>") for sequence in df[sequence_variable].values]

    def get_node_pairs_from_sequence_list(self, page_list):
        """
        Build node pairs (edges) from a list of page hits/content_ids. For example for page_list = ["A", "B", "C"],
        pairs ["A", "B"] and ["B", "C"] will be returned.
        :param page_list: list of page hits either identified using page_path or content_id
        :return: list of node pairs in page_list (list of edges)
        """
        return [[page, page_list[i + 1]] for i, page in enumerate(page_list) if i < len(page_list) - 1]

    def create_node_id_mapper(self, edge_list):
        """
        Generate a dictionary, assigning a node_id to each content item (node), which can be represented in the edge_list
        using either using content_ids or page_paths.
        :param edge_list: list of edges (node-pairs)
        :return: dictionary of nodes {content_id : node_index}
        """
        self.logger.debug("Creating node_id  list...")
        node_id = 0
        node_dict = {}

        for keys, _ in edge_list.items():
            for key in keys:
                if key not in node_dict.keys():
                    node_dict[key] = node_id
                    node_id += 1
        return node_dict

    def node_writer(self, filename, header, node_id, node_attr):
        with gzip.open(filename, "w") as file:
            file.write(header.encode())
            for node, nid in node_id.items():
                file.write("{}\t{}".format(node, nid).encode())
                if node_attr is not None:
                    file.write("\t{}".format(node_attr[node]).encode())
                file.write("\n".encode())

    def edge_writer(self, filename, header, edges, node_id, node_attr):
        with gzip.open(filename, "w") as file:
            file.write(header.encode())
            for key, value in edges.items():
                file.write("{}\t{}\t{}\t{}\t{}".format(key[0], node_id[key[0]], key[1], node_id[key[1]], value).encode())
                if node_attr is not None:
                    file.write("\t{}\t{}".format(node_attr[key[0]], node_attr[key[1]]).encode())
                file.write("\n".encode())

    # def eval_cols(df, column_to_eval='content_id_list'):
    #     if isinstance(df[column_to_eval].iloc[0], str) and any(["," in val for val in df[column_to_eval].values]):
    #         df[column_to_eval] = df[column_to_eval].map(literal_eval)
    #     return df

    # TODO this should be done in the SQL query. Need to check the Occurrences between UNTESTED QUERY (no PageSequence) and QUERY.
    def compute_occurrences(self, df, page_sequence='CIDSequence'):
        """
        conts the number of times the content_id_sequence was used.
        :param df: Pandas DataFrame containing Occurences and CIDSequence columns
        :param page_sequence: Name of column to identify the sequence (default is content_id sequence but page_path
        can also be used)
        :return: Pandas Series
        """
        self.logger.debug("Computing specialized occurrences based on  \"{}\"...".format(page_sequence))
        return df.groupby(page_sequence)['Occurrences'].transform('sum')

    # TODO this should be in the SQL too
    def get_unique_sequences_and_weights(self, df):
        if 'frequency_of_sequence_use' not in df.columns:
            df['frequency_of_sequence_use'] = self.compute_occurrences(df)
        df.drop_duplicates('CIDSequence', keep="first", inplace=True)
        df.reset_index(drop=True, inplace=True)
        logging.debug("df shape  \"{}\"...".format(df.shape))
        return df

    # TODO delooping as default
    def get_edges_and_weights(self, df):
        """
        Generate edge list where each key (edge/node pair) has a value representing the frequency of use of that edge
        (edge weight). Since an edge can be represented in multiple sequences, the Occurrences of each sequence
        containing an edge are summed.

        :param df: pandas DataFrame containing CIDSequence column which identifies session-level page hit order using
        content_ids, each page hit separated using >>
        :return: collections Counter {(source_content_id, target_content_id): edge_weight}
        """

        df = self.get_unique_sequences_and_weights(df)
        logging.debug("df shape after unique \"{}\"...".format(df.shape))
        logging.debug("df cols \"{}\"...".format(df.columns))

        edge_list_counter = Counter()

        for row in df.itertuples(index=False):
            for edge in row.node_pairs:
                edge_list_counter[tuple(edge)] += row.frequency_of_sequence_use
        logging.debug("counter len \"{}\"...".format(len(edge_list_counter)))


        return edge_list_counter
