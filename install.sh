#!/bin/bash
set -eux
set -o pipefail

WORDLIST_DIR=/user/$USER/wordlists

echo ">>>>> Installing pip requirements"
pip install -U pip pandas numpy pyarrow nltk

echo ">>>>> Aquiring Dutch wordlist"
hdfs dfs -mkdir -p $WORDLIST_DIR

if hdfs dfs -ls $WORDLIST_DIR/nl-words.txt; then
	echo "Already obtained Dutch wordlist"
else
	echo "Downloading Dutch wordlist"
	curl https://raw.githubusercontent.com/OpenTaal/opentaal-wordlist/master/wordlist.txt | \
		hdfs dfs -put - $WORDLIST_DIR/nl-words.txt
fi

echo "<<<<< Done"
exit
