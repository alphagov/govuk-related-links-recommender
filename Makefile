DATA_DIR=./data


all: ${DATA_DIR}/suggested_related_links.json

# Input: mongodb
# Output:
#  - content_id_base_path_mapping.json
#  - page_content_id_mapping.json
#  - eligible_source_content_ids.pkl
#  - eligible_target_content_ids.pkl
#  - structural_edges.csv

get_content_store_outputs: ${DATA_DIR}/content_id_base_path_mapping.json ${DATA_DIR}/page_content_id_mapping.json
get_content_store_outputs: ${DATA_DIR}/eligible_source_content_ids.plk ${DATA_DIR}/eligible_target_content_ids.plk
get_content_store_outputs: ${DATA_DIR}/structural_edges.csv

get_content_store_outputs:
	python3.6 src/data_preprocessing/get_content_store_data.py
#	aws s3 cp ${DATA_DIR}/structural_edges.csv s3://${RELATED_LINKS_BUCKET}/structural_edges.csv


# Input: Big query
# Output: functional_edges.csv
${DATA_DIR}/functional_edges.csv:
	python3.6 src/data_preprocessing/make_functional_edges_and_weights.py
#	aws s3 cp ${DATA_DIR}/functional_edges.csv s3://${RELATED_LINKS_BUCKET}/functional_edges.csv


# Output: network.csv
${DATA_DIR}/network.csv: ${DATA_DIR}/functional_edges.csv ${DATA_DIR}/structural_edges.csv
	python3.6 src/features/make_network.py
#	aws s3 cp ${DATA_DIR}/network.csv s3://${RELATED_LINKS_BUCKET}/network.csv


# Output:
# - n2v.model
# - n2v_node_embeddings
${DATA_DIR}/n2v.model: ${DATA_DIR}/network.csv
	python3.6 src/models/train_node2vec_model.py
#	aws s3 cp ${DATA_DIR}/n2v.model s3://${RELATED_LINKS_BUCKET}/n2v.model


# Input:
#  - n2v.model
#  - content_id_base_path_mapping.json
#  - eligible_source_content_ids.pkl
#  - eligible_target_content_ids.pkl
${DATA_DIR}/suggested_related_links.csv ${DATA_DIR}/suggested_related_links.json:
	python src/models/predict_related_links.py
#	TIMESTAMP=$(shell date +%Y%m%d)
#	aws s3 cp ${DATA_DIR}/suggested_related_links.json s3://$RELATED_LINKS_BUCKET/${TIMESTAMP}suggested_related_links.json
#	aws s3 cp ${DATA_DIR}/suggested_related_links.csv s3://$RELATED_LINKS_BUCKET/${TIMESTAMP}suggested_related_links.csv
