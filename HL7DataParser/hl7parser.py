#!/Users/vjain/anaconda/bin/python
import hl7
import sys

message = sys.stdin
message = message.read()

#script, filename = sys.argv
#file = open(filename)
#message = file.read()

h = hl7.parse(message)
print h
