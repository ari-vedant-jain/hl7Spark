#!/bin/bash
HOST=$1

nc -w 3 $HOST 6661 < ../HL7-Task-Force-Examples/SMOKING_Former_Smoker.xml
