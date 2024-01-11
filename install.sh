#!/bin/bash
set -eux
set -o pipefail

WORDLIST_DIR=/user/$USER/wordlists

echo ">>>>> Installing pip requirements"
pip install -U pip pandas numpy

echo ">>>>> Aquiring Dutch wordlist"
hdfs dfs -mkdir -p $WORDLIST_DIR
curl https://raw.githubusercontent.com/OpenTaal/opentaal-wordlist/master/wordlist.txt | \
	hdfs dfs -put - $WORDLIST_DIR/nl-words.txt

echo "<<<<< Done"
exit
