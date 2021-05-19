#!/bin/bash

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
#pyenv local 3.6.9
export DATA_DIR=./data
export MODEL_DIR=./models

if [[ ! "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
    echo "Please set GOOGLE_APPLICATION_CREDENTIALS to where your google credentials file is located"
    echo "e.g. export GOOGLE_APPLICATION_CREDENTIALS=tmp/creds.json"
    exit
fi

poetry run python src/run_all.py
