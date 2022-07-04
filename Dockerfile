# This Dockerfile is used to run the pytest unit tests
# To build the container:
#   docker build -t related-links-generation-tests .
# to run the tests:
#   docker run -t related-links-generation-tests
# The github action in .github/workflows/ci.yml runs the same commands

FROM ubuntu:18.04

RUN apt-get update -y
RUN apt-get install -y python3.6 python3-pip git
RUN python3.6 -m pip install --upgrade setuptools

WORKDIR /opt
ADD . .
RUN python3.6 -m pip install -r requirements-docker.txt
CMD flake8 src && pytest tests/unit
