#!/bin/bash
FILES=../messages/*
for f in $FILES
do
  #echo "Processing $f file..."
  # take action on each file. $f store current file name
  
  cat $f
done
