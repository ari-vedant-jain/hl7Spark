#!/bin/bash
FILES=tmp/hl7-messages
sudo rm $FILES/*
sudo java -cp hl7-generator-1.0-SNAPSHOT-shaded.jar  com.hortonworks.example.Main 100 $FILES
for f in $FILES/*
	do ./2vsimHL7.sh localhost $f
done
