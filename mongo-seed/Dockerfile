FROM mongo:4.0

RUN apt-get update && apt-get install -y wget && \
    wget -q https://downloads.mongodb.com/compass/mongodb-mongosh_1.1.0_amd64.deb && \
    dpkg -i mongodb-mongosh_1.1.0_amd64.deb

# TODO later: replace with s3 cp or use volume
COPY mongodump.tgz .
COPY populate-db.sh .



CMD ./populate-db.sh
