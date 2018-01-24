#!/bin/sh
sudo rm -rf tmp/hl7-messages/*
java -cp hl7-generator-1.0-SNAPSHOT-shaded.jar  com.hortonworks.example.Main 10000 tmp/hl7-messages/
