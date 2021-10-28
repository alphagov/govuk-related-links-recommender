#!/bin/bash

export IP=52.50.249.43
cd ~/govuk
scp -i ~/.ssh/max-data-labs.pem content_items.bson.gz bigquery.json ubuntu@${IP}:



ssh -i ~/.ssh/max-data-labs.pem -o StrictHostKeyChecking=no ubuntu@${IP} << EOF
  echo "Connected!"

  # Setup data directory
  sudo mkdir /var/data
  sudo chown -R ubuntu:ubuntu /var/data

  # Setup Github directory
  mkdir /var/data/github
  cd /var/data/github

  echo "Cloning recommender repo"
  # Clone related links repository
  git clone https://github.com/alphagov/govuk-related-links-recommender.git
  cd govuk-related-links-recommender
  git switch python3.8

  # Install updates and required packages
  sudo apt-get update -y
  sudo apt-get install -y jq python3-pip

  # Install Mongo
  sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4

  echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.0.list
  sudo apt-get update -y
  sudo apt-get install -y mongodb-org
  sudo service mongod start

  cd
  gunzip content_items.bson.gz
  mongorestore -d content_store -c content_items content_items.bson

  cd /var/data/github/govuk-related-links-recommender
  pip3 install -r requirements.txt

  export DATA_DIR=$PWD/data
  mkdir -p $DATA_DIR # make sure it exists
  export GOOGLE_APPLICATION_CREDENTIALS=~/bigquery.json

  python3.8 src/data_preprocessing/get_content_store_data.py
  python3.8 src/data_preprocessing/make_functional_edges_and_weights.py
  python3.8 src/features/make_weighted_network.py
  python3.8 src/models/train_node2vec_model.py
  python3.8 src/models/predict_related_links.py
EOF

scp -r -i ~/.ssh/max-data-labs.pem ubuntu@${IP}:/var/data/github/govuk-related-links-recommender-data ~/tmp/
