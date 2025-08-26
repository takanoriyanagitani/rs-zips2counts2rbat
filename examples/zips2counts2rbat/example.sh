#!/bin/sh

export ENV_DIR_NAME=./sample.d

geninput(){
	echo generating input files...

	mkdir -p "${ENV_DIR_NAME}"

	echo hw0 > ./sample.d/hw0.txt
	echo hw1 > ./sample.d/hw1.txt

	echo hw2 > ./sample.d/hw2.txt
	echo hw3 > ./sample.d/hw3.txt

	ls ./sample.d/hw[01].txt | zip -@ ./sample.d/00000001.zip
	ls ./sample.d/hw[23].txt | zip -@ ./sample.d/00000002.zip
}

test -f "${ENV_DIR_NAME}/00000001.zip" || geninput
test -f "${ENV_DIR_NAME}/00000002.zip" || geninput

./zips2counts2rbat
