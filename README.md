govuk-related-links-recommender
==============================

> This repo was archived in October 2023 as it [hasn't been running anywhere since April 2023](https://github.com/alphagov/govuk-puppet/commit/54e72f6785224d4fb2eb6be44aa8c443aaa40c63), and prior to that, had been broken for "several months" on Jenkins (where [it had been running since December 2021](https://github.com/alphagov/govuk-puppet/pull/11337)).

Main things this repo does:

1) Training an ML model for creating suggested related links like these (on the right sidebar) https://www.gov.uk/government/collections/health-certificates-for-animal-and-animal-product-imports-to-great-britain
2) Getting suggested recommended links from the trained model
3) Uploading the model, data, logs and recommended links to aws
4) Feeding links from AWS to the publishing api

More details here: https://docs.publishing.service.gov.uk/manual/related-links.html

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.testrun.org


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>


## Python environment
Need to set and activate environment variables before running pipeline. To run the make_functional_edges_and_weights.py
script, use command line to `export GOOGLE_APPLICATION_CREDENTIALS='path/to/GOV-UK BigQuery analytics-fa2ed548d98c.json'`

Environment variables are also used to locate data and model directories: DATA_DIR and MODEL_DIR.
Make sure these are activated before running data pipeline.


## Config

### Preprocessing config

`src/config/preprocessing-config.yml`

Set the names for the structural (hyperlink) and functional (user movement) graphs/networks output csvs, from/to dates for GA data, if you want to use intraday data (cheap and quick!), and where the mongodb instance containing the content store is serving from

### Node2vec config

`src/config/node2vec-config.yml`

Set whether you want to use weighted edges in node2vec (weights=user movement), various hyperparameters and output filenames for the model and embeddings


### Important: Output file names

The names of the files that get uploaded to AWS (and ingested into the content api) are hardcoded in `run_link_generation` and `run_link_ingestion`.
So any other file name (ie youve changed it in `node2vec-config.yml`) won't get picked up.

## Running the complete pipeline (high level)

### Run link generation

`run_link_generation.sh`

1) Downloads content store backup from aws
2) sets up mongodb of content store
3) Runs `run_all.py` (see below for the contents of that)
4) Uploads to aws

### Run link ingestion

`run_link_ingestion.sh`

1) Feeds links from AWS to the publishing api

## Running the pipeline (low level)

Here's a run through of what `run.py` does:


### Get the functional network

`src/datapreprocessing/make_functional_edges_and_weights.py`

The functional network is how users move around gov.uk. Function queries `govuk-bigquery-analytics` for a list of source and destination content ids and a count of how many times users made that transition. Outputs a csv of weighted source-destination pairs. See `src/config/preprocessing-config.yml`
for settings. Runs quickly. Runs cheaply if you use `use_intraday: True` in the config.

### Get the structural network

`src/data_preprocessing/get_content_store_data.py`

The structural network is the network of hyperlinks in gov.uk. Function assumes there is a mongodb instance containing the content store on the ip and port configured in `src/config/preprocessing-config.yml`

Extracts all the links from content store content. Outputs csv of unweighted source - destination pairs. Seems extremely faffy and takes a long time.

How to set up mongodb instance locally: https://github.com/ukgovdatascience/govuk-mongodb-content

### Create functional + structural network

`src/features/make_weighted_network.py`

Concatenates the structural and functional edgelists. Untravelled edges (ie from the structural network) get assigned `weight = 1`. Everything else has `weight = n users that travelled that edge`

Outputs csv of unique weighted source-destination pairs

### Training node2vec

`src/models/train_node2vec_model`

Config in `src/config/node2vec-config.yml`. Trains the model and saves the embeddings and model.

### Predict links

`src/models/predict_related_links`

Gets the top 5 most similar nodes for every node (not explicitly excluded in the `source_exclusions_that_are_not_linked_from.yml` ) ! Filters out results subject to lots of uncertainty (ie low user traversal). Outputs a csv


## Rules for displaying automated related links
**Do-not-display-automated-related-links-from these-pages**
- items with curated related links (ordered-related-items)
- document_type=="fatality_notice"
- document_type=="smart_answer" [except we have previously identified these looking for string matches]
```
(~related_links.link.str.contains("/y/")) &
                              (~related_links.link.str.endswith("/y")) &
                              (~related_links.link.str.contains("/no/")) &
                              (~related_links.link.str.endswith("/no")) &
                              (~related_links.link.str.contains("/education/"))
```
- '/contact-student-finance-england'
- '/student-finance-forms'
- '/funding-for-postgraduate-study'
- '/advanced-learner-loan'
- '/teacher-training-funding'
- '/extra-money-pay-university'
- '/nhs-bursaries'
- '/travel-grants-students-england'
- '/career-development-loans'
- '/care-to-learn'
- '/dance-drama-awards'
- '/social-work-bursaries'
- '/apply-for-bankruptcy'
- '/view-driving-licence'
- '/being-taken-to-employment-tribunal-by-employee'
- '/employment-tribunals'
- '/staying-uk-eu-citizen'
- '/report-driving-medical-condition'
- '/check-flood-risk'

**do-not-suggest-these-links-from-other-pages**
- document_type is in this list: ["consultation_outcome",
                     "closed_consultation",
                     "open_consultation",
                     "fatality_notice"]
- '/apply-for-bankruptcy'
- '/view-driving-licence'
- '/being-taken-to-employment-tribunal-by-employee'
- '/employment-tribunals'
- '/staying-uk-eu-citizen'
- '/report-driving-medical-condition'
- '/check-flood-risk' (edited)

## Licence

[MIT License](LICENCE)

