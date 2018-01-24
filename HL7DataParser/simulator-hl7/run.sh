#!/bin/bash
for i in {1..7}; do ./2vsimHL7.sh localhost ../messages/sample$i.hl7; done
