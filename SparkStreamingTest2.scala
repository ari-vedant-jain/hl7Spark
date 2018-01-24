package poc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions
import org.apache.spark.storage.StorageLevel

import kafka.serializer.{DefaultDecoder, StringDecoder}
//import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.KafkaUtils
import _root_.kafka.serializer.StringDecoder

object SparkStreamingTest2 extends Serializable{
     //val hbaseConf = HBaseConfiguration.create()
 @transient private var instance: SQLContext = null
  
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }

  
  
     def main(args: Array[String]) {
       
 val conf = new SparkConf().setAppName("Ingesting HL7 from Kafka")
  conf.set("spark.streaming.ui.retainedBatches", "5")
  val zookeeperurl = args(1)
// Enable Back Pressure
      conf.set("spark.streaming.backpressure.enabled", "true")
      
       System.out.println(" ------------- 1 ------------");
 
       //val sc = new SparkContext(conf)
       //val sqlContext = new SQLContext(sc)
        
       
      val ssc = new StreamingContext(conf, batchDuration = Seconds(15))
     
      // Enable checkpointing
      ssc.checkpoint("checkpoint")
       System.out.println(" ------------- 2 ------------");
      // Connect to Kafka

      val kafkaParams = Map("metadata.broker.list" -> "vjain-perm9.field.hortonworks.com:6667,vjain-perm2.field.hortonworks.com:6667", 
          "zookeeper.connect" ->  "vjain-perm0.field.hortonworks.com:2181,vjain-perm1.field.hortonworks.com:2181,vjain-perm2.field.hortonworks.com:2181",
          "group.id" -> "12345"    
      )
      
      val kafkaTopics = Set("hl7-messages")
       System.out.println(" ------------- 3 ------------");
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)
       System.out.println(" ------------- 4 ------------");
      // print 10 last messages
      
      val lines = messages.map(_._2)
      
      
      lines.print()
      //messages.print()
      
      
      
      lines.foreachRDD(rdd => if(!rdd.partitions.isEmpty){
               System.out.println("Kicking off Streaming job. ")
                 try{ 
                   val rdd1 = rdd
		                 	val sqlContext = SparkStreamingTest2.getInstance(rdd.sparkContext)
										  import sqlContext.implicits._
                
                    
                       val dfs = sqlContext.read.json(rdd1) 
                       dfs.printSchema()
                       val dfsSegments = dfs.select(dfs.col("address").as("dfsaddress"),
                           dfs.col("date").as("dfsdate"),
                           dfs.col("id").as("dfsid"),
                           dfs.col("name").as("dfsname"),
                        org.apache.spark.sql.functions.explode(dfs.col("segments")).as("segmentsExplode")
                        );
              
                        val dfsSegmentsData = dfsSegments.select(dfsSegments.col("dfsaddress"),
                        dfsSegments.col("dfsdate"),
                        dfsSegments.col("dfsid"),
                        dfsSegments.col("dfsname"),
                        dfsSegments.col("segmentsExplode").getField("FC").as("dfsegmentsfc"), 
                        dfsSegments.col("segmentsExplode").getField("Rep").as("dfsegmentsrep"),
                        dfsSegments.col("segmentsExplode").getField("Seq").as("dfsegmentsseq"),
                        dfsSegments.col("segmentsExplode").getField("VF").as("dfsegmentsvf"),
                        dfsSegments.col("segmentsExplode").getField("Val").as("dfsegmentsval"),
                        dfsSegments.col("segmentsExplode").getField("_id").as("dfsegmentsid"),
                        org.apache.spark.sql.functions.explode(dfsSegments.col("segmentsExplode").getField("Fields")).as("segmentsFieldsExplode")
                        );

                        val dfsSegmentsFieldsData = dfsSegmentsData.select(dfsSegments.col("dfsaddress"),
                        dfsSegmentsData.col("dfsdate"),
                        dfsSegmentsData.col("dfsid"),
                        dfsSegmentsData.col("dfsname"),
                        dfsSegmentsData.col("dfsegmentsfc"), 
                        dfsSegmentsData.col("dfsegmentsrep"),
                        dfsSegmentsData.col("dfsegmentsseq"),
                        dfsSegmentsData.col("dfsegmentsvf"),
                        dfsSegmentsData.col("dfsegmentsval"),
                        dfsSegmentsData.col("dfsegmentsid"),
                        dfsSegmentsData.col("segmentsFieldsExplode").getField("Val").as("dfsegmentsfieldsval"),
                        dfsSegmentsData.col("segmentsFieldsExplode").getField("_id").as("dfsegmentsfieldsid")
                        );                       
                        
                       dfsSegmentsFieldsData.printSchema()
                       dfsSegmentsFieldsData.show()
                       dfsSegmentsFieldsData.registerTempTable("dfsSegmentsFieldsData")
                       
                       val patientsQuery = "select distinct dfsid as ID, dfsname as NAME, dfsaddress as ADDRESS from dfsSegmentsFieldsData"
                       val patientsData = sqlContext.sql(patientsQuery)
                       System.out.println(" -----------Saving PATIENT data in HBase----------- patientsData: # of rows: "+ patientsData.count());
                       patientsData.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "PATIENT.PATIENTMETA","zkUrl" -> zookeeperurl))
                      System.out.println(" -----------Saved PATIENT data in HBase----------- ");

                      val hl7messagesQuery = "select distinct dfsid as PID, dfsegmentsid as SEGMENTSID, dfsegmentsseq as SEGMENTSSEQ, dfsegmentsfieldsid as SEGMENTSFIELDSID, dfsdate as DATEVALUE, dfsegmentsval as SEGMENTSVALUE, dfsegmentsfieldsval as SEGMENTSFIELDSVALUE from dfsSegmentsFieldsData"
                      val hl7messageData = sqlContext.sql(hl7messagesQuery)

                       System.out.println(" -----------Saving HL7MESSAGES data in HBase----------- hl7messageData: # of rows: "+ hl7messageData.count());
                      hl7messageData.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "PATIENT.HL7MESSAGES","zkUrl" -> zookeeperurl))
                      System.out.println(" -----------Saved HL7MESSAGES data in HBase----------- ");

                       
                     }catch{
                    case e:Exception => e.printStackTrace()
                  
               
                 }
             System.out.println("End of batch........")  
       })
      
      
      
      // start streaming computation
      ssc.start
       System.out.println(" ------------- 5 ------------");
        ssc.awaitTermination()
     }
    

     
}
