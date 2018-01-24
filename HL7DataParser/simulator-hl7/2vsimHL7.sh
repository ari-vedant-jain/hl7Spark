#!/bin/bash
HOST=$1
FILE=$2
nc $HOST 6667 < $FILE
#cat $FILE | nc -u localhost 6661 
