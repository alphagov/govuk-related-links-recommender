#!/bin/bash

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
if ! command -v pyenv > /dev/null 2>&1
then
    echo "pyenv could not be found. Make sure it's installed and in your PATH"
    echo "see https://github.com/pyenv/pyenv-installer"
    exit
fi
eval "$(pyenv init -)"
pyenv install -s 3.6.9
pyenv local 3.6.9


export LANG=C.UTF-8
export PATH=$HOME/.poetry/bin:${PATH}
if ! command -v poetry > /dev/null 2>&1
then
    echo "poetry could not be found. Make sure it's installed and in your PATH"
    echo "see https://python-poetry.org/docs/#installation"
    exit
fi

. $HOME/.poetry/env
poetry install

echo "All done"
