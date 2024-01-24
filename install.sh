#!/bin/bash
set -euo pipefail

# cd to the directory of this script
cd $( dirname "$0" )

## Setup virtual environment
if [ ! -d venv ]; then
    python3 -m virtualenv venv
fi

source venv/bin/activate # activate venv

set -x # @echo on

# Install requirements
pip install -U pip pandas numpy pyarrow nltk pyspark-pandas venv-pack pyspark==3.4.1

# Put venv in archive (passed to executors with "spark-submit --archives [...]")
venv-pack -o pyspark_venv.tar.gz

# Install English wordlist
WORDLIST_DIR=/user/$USER/wordlists
python3 -c "import nltk; nltk.download('words')"

# Install Dutch wordlist
hdfs dfs -mkdir -p $WORDLIST_DIR
if hdfs dfs -ls $WORDLIST_DIR/nl-words.txt; then
	;
else
	curl https://raw.githubusercontent.com/OpenTaal/opentaal-wordlist/master/wordlist.txt | \
		hdfs dfs -put - $WORDLIST_DIR/nl-words.txt
fi

