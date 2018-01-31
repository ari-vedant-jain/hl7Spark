# hl7Spark
Analyze HL7 data using Spark Streaming

This is an implementation of analyzing HL7 data using Spark. The architecture is as follows: 

HL7 messages simulator -> TCP Port/Remote Disk -> Python Parser (converts to JSON) -> Spark Streaming -> HBase (persistant Storage) 


In order to generate sample data onto local disk, run this file: 

        ./HL7DataParser/simulator-hl7/run.sh
        
Once you have the data on the local disk, you can use this python program to parse the individual messages:

        python HL7DataParser/hl72JSON.py

Spark Streaming reads the data from a kafka topic. It takes care of converting the JSON payload into 2 HBase tables. 
You will need to integrate the python program to write to a Kafka topic. 

The Hbase tables are with the following schema:
HBase Tables:

Patient Contact Details: 
     - Patient ID
     - Patient Name
     - Address 
     

HL7 Events Table 
     - Patient ID 
     - Segment_
     - Value
     - Date
     
     
You will need to create those tables in HBase. Following are the DDL's I used using Phoenix ->

DROP TABLE IF EXISTS DETAILS.PATIENT;
create table DETAILS.PATIENT (
        id VARCHAR not null,
        “cf".name VARCHAR,
        “cf”.address VARCHAR,
        CONSTRAINT PK PRIMARY KEY(id));


DROP TABLE IF EXISTS DETAILS.HL7MESSAGES;
create table DETAILS.HL7MESSAGES (
        pid VARCHAR not null,
        segment VARCHAR,
        value VARCHAR,
        date DATE,
        CONSTRAINT PK PRIMARY KEY(pid, date, segment));


DROP TABLE IF EXISTS DETAILS.HL7MESSAGES;
create table HL7MESSAGES (
     pid VARCHAR not null,
     segmentsid VARCHAR not null,
     segmentsseq smallint not null,
     segmentsfieldsid VARCHAR not null,
     datevalue DATE not null,
     segmentsvalue VARCHAR,
     segmentsfieldsvalue VARCHAR, 
     CONSTRAINT PK PRIMARY KEY(pid, segmentsid, segmentsseq, segmentsfieldsid, datevalue));


Create a kafka topic:
kafka-topics.sh --zookeeper localhost:2181   --alter --topic hl7-messages --config retention.ms=1000

Before you start the Spark streaming program, you'll need to makes sure to have all the dependencies and hbase jars in the classpath. Like this: 

spark-submit  --class poc.SparkStreamingTest2 —conf "spark.executor.extraJavaOptions=-Dsun.io.serialization.extendedDebugInfo=true" —master yarn-cluster --files=/usr/hdp/current/hive/conf/hive-site.xml,/usr/hdp/current/hbase/conf/hbase-site.xml, --jars /usr/hdp/current/hbase/lib/hbase-server.jar,/usr/hdp/current/hbase/lib/hbase-client-1.1.2 .current.jar,/usr/hdp/current/hbase/lib/hbase-common-1.1.2.current.jar,/usr/hdp/current/hbase/lib/hbase-protocol-1.1.2.current.jar,/usr/hdp/current/phoenix/lib/phoenix-
core-4.4.0.current.jar,/usr/hdp/current/phoenix/lib/phoenix-spark-4.4.0.current.jar,/usr/hdp/current/spark/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark/lib/datanucleus-core-3.2.10.jar,/usr/hdp/current/spark/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp
/current/hive/lib/hive-exec-1.2.1000.current.jar /hl7Spark/Spark-POC-0.0.2.jar hdfs:///user/spark/sparkJobJsonInput localhost:2181:/hbase-unsecure 15




