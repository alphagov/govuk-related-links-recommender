#!/bin/bash

set -ueo pipefail



function file_or_die {
    if [ -s $1 ]; then
        echo "Found $1. All good"
    else
        echo "$1 not found or is empty. Exiting"
        exit 255
    fi
}


echo "Python version installed:"
python3.8 -V


echo -e "\n\n### Starting related links generation process\n"

echo -e "\n\n# Generating structural edges\n"
# Input: mongodb
python3.8 src/data_preprocessing/get_content_store_data.py


file_or_die "$DATA_DIR/structural_edges.csv"
file_or_die "$DATA_DIR/content_id_base_path_mapping.json"
file_or_die "$DATA_DIR/page_path_content_id_mapping.json"
file_or_die "$DATA_DIR/eligible_source_content_ids.pkl"
file_or_die "$DATA_DIR/eligible_target_content_ids.pkl"
#aws s3 cp $DATA_DIR/structural_edges.csv s3://$RELATED_LINKS_BUCKET/structural_edges.csv
#aws s3 cp $DATA_DIR/content_id_base_path_mapping.json s3://$RELATED_LINKS_BUCKET/content_id_base_path_mapping.json
#aws s3 cp $DATA_DIR/page_path_content_id_mapping.csv s3://$RELATED_LINKS_BUCKET/page_path_content_id_mapping.csv
#aws s3 cp $DATA_DIR/eligible_source_content_ids.pkl s3://$RELATED_LINKS_BUCKET/eligible_source_content_ids.pkl
#aws s3 cp $DATA_DIR/eligible_target_content_ids.pkl s3://$RELATED_LINKS_BUCKET/eligible_target_content_ids.pkl

echo -e "\n\n# Generating functional edges\n"
# Input: Big query
python3.8 src/data_preprocessing/make_functional_edges_and_weights.py
file_or_die "$DATA_DIR/functional_edges.csv"
#aws s3 cp $DATA_DIR/functional_edges.csv s3://$RELATED_LINKS_BUCKET/functional_edges.csv

echo -e "\n\n# Running make_network\n"
# Input:
#  - structural_edges.csv
#  - functional_edges.csv
# Output: network.csv
ls -l src
ls -l src/features
python3.8 src/features/make_weighted_network.py
file_or_die "$DATA_DIR/network.csv"
#aws s3 cp $DATA_DIR/network.csv s3://$RELATED_LINKS_BUCKET/network.csv

echo -e "\n\n# Running train_node2vec_model\n"
# Input: network.csv
#python3.8 src/models/train_node2vec_model.py
file_or_die "$DATA_DIR/n2v.model"
file_or_die "$DATA_DIR/n2v_node_embeddings"
file_or_die "$DATA_DIR/n2v.model.wv.vectors.npy"
# NOTE: Word2Vec.save() can generate more files, with unspecified names. See
# https://stackoverflow.com/questions/47173538/why-are-multiple-model-files-created-in-gensim-word2vec
#aws s3 cp $DATA_DIR/n2v.model s3://$RELATED_LINKS_BUCKET/n2v.model
#aws s3 cp $DATA_DIR/n2v_node_embeddings s3://$RELATED_LINKS_BUCKET/n2v_node_embeddings
#aws s3 cp $DATA_DIR/n2v.model.wv.vectors.npy s3://$RELATED_LINKS_BUCKET/n2v.model.wv.vectors.npy


echo -e "\n\n# Running predicted_related_links\n"
# Input:
#  - Big Query
#  - n2v.model
#  - content_id_base_path_mapping.json
#  - eligible_source_content_ids.pkl
#  - eligible_target_content_ids.pkl
python3.8 src/models/predict_related_links.py
file_or_die "${DATA_DIR}/suggested_related_links.json"
file_or_die "${DATA_DIR}/suggested_related_links.tsv"
TIMESTAMP=`date +%Y%m%d`
#aws s3 cp ${DATA_DIR}/suggested_related_links.json s3://$RELATED_LINKS_BUCKET/${TIMESTAMP}suggested_related_links.json
#aws s3 cp ${DATA_DIR}/suggested_related_links.tsv s3://$RELATED_LINKS_BUCKET/${TIMESTAMP}suggested_related_links.tsv

echo -e "\n\n# List out all generated data files\n"
ls -lR $DATA_DIR

#echo -e "\n\n# Copying logs to S3\n"
#aws s3 cp /tmp/govuk-related-links-recommender.log s3://$RELATED_LINKS_BUCKET/govuk-related-links-recommender.log
#aws s3 cp /var/tmp/related_links_process.log s3://$RELATED_LINKS_BUCKET/related_links_generation.log

echo "related_links process succeeded"
