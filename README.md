# hl7Spark
Analyze HL7 data using Spark Streaming

This is an implementation of analyzing HL7 data using Spark. The architecture is as follows: 

HL7 messages simulator -> TCP Port/Remote Disk -> Python Parser (converts to JSON) -> Spark Streaming -> HBase (persistant Storage) 


