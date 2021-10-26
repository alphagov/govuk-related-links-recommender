FROM mongo:4.0

#VOLUME ["/home/mf/govuk/govuk-related-links-recommender/content-store-data"]

RUN apt-get update && apt-get install -y wget && \
    wget -q https://downloads.mongodb.com/compass/mongodb-mongosh_1.1.0_amd64.deb && \
    dpkg -i mongodb-mongosh_1.1.0_amd64.deb

COPY populate-db.sh .


CMD ./populate-db.sh
