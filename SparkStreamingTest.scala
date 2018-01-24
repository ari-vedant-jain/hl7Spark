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
/*
/**
 * @author VikasDesktop
 */
object SparkStreamingTest extends Serializable{
     val hbaseConf = HBaseConfiguration.create()

     def main(args: Array[String]) {
       
        if (args.length != 5) {
         // System.err.println("This Job accepts 5 parameters<priorJobDoneDir><hiveSourceDir><hiveTargetDir><batchinterval><zookeeperurl>")
          //System.exit(1)
      }
       
       val conf = new SparkConf()
        .setAppName("PhoenixLoaderStreamingJob")
       //.setMaster("local[1]")
       val sc = new SparkContext(conf)
       val sqlContext = new SQLContext(sc)
       val ssc = new StreamingContext(sc, Seconds(args(2).toInt))
       val zookeeperurl = args(1)
       //val hiveContext = new HiveContext(sc)
       //ssc.addStreamingListener(new Listener(conf))
       val sampleRecords = args(0)
       
       val hadoopConfig = new Configuration()
       val fs = FileSystem.get(hadoopConfig)
       var locationTurnOn = false
       
       val lines = ssc.textFileStream(sampleRecords)
       
       lines.foreachRDD(rdd => if(!rdd.partitions.isEmpty){
               System.out.println("Kicking off Streaming job. ")
                 try{    
                       val dfs = sqlContext.read.json(rdd) 
                       dfs.printSchema()
                       dfs.show(1)
                       dfs.registerTempTable("dfs")
                       System.out.println(" ---------------------- ");
                       

                      val dfsDataQuery = "SELECT id, address, segments.FC, segments.Rep, segments.Seq, segments.VF, segments.Val, segments._id from dfs"
                      val dfsData = sqlContext.sql(dfsDataQuery)
                      dfsData.show(3)
                       println(" ****************************");
                     }catch{
                    case e:Exception => e.printStackTrace()
                  }
                 
             System.out.println("End of batch........")
       })
        ssc.start()
        ssc.awaitTermination()
     }
}


/*
 * Kicking off Streaming job. 
root
 |-- address: string (nullable = true)
 |-- date: string (nullable = true)
 |-- id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- segments: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- FC: long (nullable = true)
 |    |    |-- Fields: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- Repetitions: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- Rep: long (nullable = true)
 |    |    |    |    |    |    |-- Val: string (nullable = true)
 |    |    |    |    |    |    |-- _id: string (nullable = true)
 |    |    |    |    |-- Val: string (nullable = true)
 |    |    |    |    |-- _id: string (nullable = true)
 |    |    |-- Rep: long (nullable = true)
 |    |    |-- Seq: long (nullable = true)
 |    |    |-- VF: long (nullable = true)
 |    |    |-- Val: string (nullable = true)
 |    |    |-- _id: string (nullable = true)

+--------------------+-------------------+------------+--------------------+--------------------+
|             address|               date|          id|                name|            segments|
+--------------------+-------------------+------------+--------------------+--------------------+
|123 Peachtree St,...|2001-03-31 00:00:00|20010422GA03|Doe John Fitzgera...|[[17,WrappedArray...|
+--------------------+-------------------+------------+--------------------+--------------------+

End of batch........

*/
*/
