import os
# import random

import networkx as nx
from node2vec import Node2Vec

# set seed for deterministic tests etc
# random.seed(1)

# stuff to save model
# DATA_DIR = os.getenv("DATA_DIR")
# models = os.path.join(os.path.dirname(DATA_DIR), "models")
#
# EMBEDDING_FILENAME = os.path.join(models, "n2v_node_embeddings")
# EMBEDDING_MODEL_FILENAME = os.path.join(models, "n2v.model")
# EDGES_EMBEDDING_FILENAME = os.path.join(models, "n2v_edge_embeddings")


def build_dict(k1,v1,k2,v2):
    agg_dict = dict(zip(k1,v1))
    for key,value in zip(k2,v2):
        if key not in agg_dict.keys():
            agg_dict[key] = value
    return agg_dict


def train_node2_vec_model(edges_df):
    nid_cid = build_dict(
        edges_df.source, edges_df.source_content_id, edges_df.target,
        edges_df.destination_content_id)
    nid_url = build_dict(
        edges_df.source, edges_df.source_base_path, edges_df.target,
        edges_df.destination_base_path)
    url_nid = build_dict(
        edges_df.source_base_path, edges_df.source,
        edges_df.destination_base_path, edges_df.target)

    # instantiate graph
    graph = nx.DiGraph()
    # add edges to graph
    for src, dest in zip(edges_df.source, edges_df.target):
        graph.add_edge(src, dest)
    # add node attributes to graph
    attrs = {nid: {"cid": nid_cid[nid], "url": nid_url[nid]
                   #               "test":False, "val":False,
                   #                "label":[]
                   } for nid in graph.nodes()}
    nx.set_node_attributes(graph, attrs)

    # Precompute probabilities and generate walks
    node2vec = Node2Vec(graph, dimensions=64, walk_length=30, num_walks=300, workers=1)
    # print(datetime.now().strftime("%H:%M:%S"), "Fitting model...")
    # Embed nodes
    model = node2vec.fit(window=10, min_count=1, batch_words=4, seed=1, workers=1)
    # Any keywords acceptable by gensim.Word2Vec can be passed, `dimensions` and `workers` are
    # automatically passed (from the Node2Vec constructor)
    # print(datetime.now().strftime("%H:%M:%S"), "Finished fitting model...")

    # Save embeddings for later use
    # model.wv.save_word2vec_format(EMBEDDING_FILENAME)
    # # Save model for later use
    # model.save(EMBEDDING_MODEL_FILENAME)

    return model
