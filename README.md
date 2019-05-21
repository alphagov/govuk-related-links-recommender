govuk-related-links-recommender
==============================

Machine learning model to recommend related content

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
Make sure these are activated before runnning data pipeline.

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

