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



/**
 * @author Gitendra
 */



object SparkStreamingTest1 extends Serializable{
 <!--
  val hbaseConf = HBaseConfiguration.create()

     def main(args: Array[String]) {
       
        if (args.length != 5) {
          //System.err.println("This Job accepts 5 parameters<priorJobDoneDir><hiveSourceDir><hiveTargetDir><batchinterval><zookeeperurl>")
          //System.exit(1)
      }
       
       val conf = new SparkConf()
        .setAppName("PhoenixLoaderStreamingJob")
       // .set("spark.driver.host","192.166.4.92")
        //.setMaster("local[1]")
       val sc = new SparkContext(conf)
       val sqlContext = new SQLContext(sc)
       val ssc = new StreamingContext(sc, Seconds(args(2).toInt))
       val zookeeperurl = args(1)
       //val hiveContext = new HiveContext(sc)
       //ssc.addStreamingListener(new Listener(conf))
       val sampleRecords = args(0)
       // val Array(broker, zk, topic) = args
       //val broker = "hbaseb03.hashmap.net:9092"                        //args(3)
       val topic =  "hl7-messages"         //args(4)
       
       val hadoopConfig = new Configuration()
       val fs = FileSystem.get(hadoopConfig)
       //var locationTurnOn = false
       val kafkaConf = Map(
            "metadata.broker.list" ->"sandbox.hortonworks.com:6667",                   //"hbaseb03.hashmap.net:6667",
            "zookeeper.connect" -> "sandbox.hortonworks.com:2181",
            "group.id" -> "test-consumer-group",
            "zookeeper.connection.timeout.ms" -> "30000")
      
       //val lines = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Set(topic)).map(_._2)
       //val lines = KafkaUtils.createStream(ssc, zookeeperurl, "kafka-spark-streaming-example", Set(topic)).map(_._2)
       
       val lines = KafkaUtils.createStream[Array[Byte], String, 
                    DefaultDecoder, StringDecoder](
                    ssc,
                    kafkaConf,
                    Map(topic -> 1),
                    StorageLevel.MEMORY_ONLY_SER).map(_._1)
                
       System.out.println("Kicking off Streaming job. lines: " + lines.print())
                    
       lines.foreachRDD(rdd => if(!rdd.partitions.isEmpty){
               System.out.println("Kicking off Streaming job. ")
                 try{    
                   System.out.println(" ------------- ");
                   System.out.println(rdd.take(0).toString());
                   System.out.println(" ------------- ");
                   
                   /*    val dfs = sqlContext.read.json(rdd) 
                       dfs.printSchema()
                       dfs.show(1)
                       val patientDF  = dfs.selectExpr("id","name","address")
                       val hl7messagesDF  = dfs.selectExpr("id","segment","value","date")
                       patientDF.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "PATIENT","zkUrl" -> zookeeperurl))
                       hl7messagesDF.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "HL7MESSAGES","zkUrl" -> zookeeperurl))
                  */
                 
                 }catch{
                    case e:Exception => e.printStackTrace()
                  }
             System.out.println("End of batch 1........")
             System.out.println("End of batch 2........")
             System.out.println("End of batch 3........")
             System.out.println("End of batch 4........")
                 
             System.out.println("End of batch 5........")
       })
        System.out.println("End of batch 6........")
        ssc.start()
        ssc.awaitTermination()
     } -->
}
