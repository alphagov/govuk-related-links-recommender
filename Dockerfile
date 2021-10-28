FROM ubuntu:18.04

# Prepare container to compile python via pyenv
RUN apt-get update -y
RUN apt-get upgrade -y

RUN DEBIAN_FRONTEND="noninteractive" apt-get install --yes python3.6 python3-distutils python3-pip # curl git # gcc make zlib1g zlib1g-dev libreadline-dev libbz2-dev libssl-dev build-essential libreadline-dev libsqlite3-dev wget llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev
# RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 10


#RUN git clone https://github.com/alphagov/govuk-related-links-recommender.git
WORKDIR govuk-related-links-recommender

# Temporary: will be in repo eventually
COPY . .


RUN pip3 install -r requirements.txt

ENV DATA_DIR=./data
ENV MODEL_DIR=./models
ENV MONGO_URL=mongodb://content-store
ENV GOOGLE_APPLICATION_CREDENTIALS="tmp/bigquery.json"

#CMD ./docker-generate
RUN pytest tests/unit
CMD /bin/bash
