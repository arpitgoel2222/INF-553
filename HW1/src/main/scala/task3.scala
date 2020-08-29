import java.io.{File, PrintWriter}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.io.FileWriter
import org.apache.spark.HashPartitioner



object task3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("inf-553_task1")
      .getOrCreate()

    val input_file = args(0)
    val output_file = args(1)
    val typep = args(2)
    val n_partitions = args(3).toInt
    val n = args(4).toInt

    val sc = spark.sparkContext;
    if(typep=="default") {
      val data = spark.read.json(input_file).rdd
      val partitions = data.getNumPartitions
      val rdd = data.map(x => (x.getAs[String]("business_id"), 1)).reduceByKey((x, y) => x + y)
      val size = rdd.glom().map(_.length).collect().toList
      val filtered_rdd = rdd.filter(x => (x._2) > n).take(n).toList
      println("------------")
      println(partitions)
      println(size)
      println(filtered_rdd)

      val file = new FileWriter(output_file)
      file.write("{")
      file.write("\"n_partitions\": " + partitions + ",")
      file.write("\"n_items\": [")
      var j=1
      for (x <- size) {
        file.write("" + x)
        j=j+1
        print(j)
        if(j==partitions+1)
        {
          print(j)
          file.write("],")
        }
        else{
          print(j)
          file.write(",")
        }

      }

      file.write("\"result\": [")
      var i = 1
      for ((k, v) <- filtered_rdd) {
        file.write("[")
        file.write("\""+k+"\"")
        file.write("," + v)
        i = i+1
        if (i == filtered_rdd.length+1) {
          file.write("]")
        } else {
          file.write("],")
        }
      }
      file.write("]")
      file.write("}")
      file.flush()




    }

    else{
      val data = spark.read.json(input_file).rdd

      val rdd1 = data.map(x => (x.getAs[String]("business_id"), 1))
      val rdd2 =  rdd1.partitionBy(new HashPartitioner(n_partitions))
      val partitions = rdd2.getNumPartitions
      val size = rdd2.glom().map(_.length).collect().toList
      val new_rdd = rdd2.reduceByKey((x, y) => x + y).filter(x => (x._2) > n).take(n).toList
      println("---------")
      println(partitions)
      println(size)
      println(new_rdd)
      val file = new FileWriter(output_file)
      file.write("{")
      file.write("\"n_partitions\": " + partitions + ",")
      file.write("\"n_items\": [")
      var j=1
      for (x <- size) {

        file.write("" + x)
        j=j+1
        if(j==partitions+1)
        {
          file.write("],")
        }
        else{
          file.write(",")
        }

      }

      file.write("\"result\": [")
      var i = 1
      for ((k, v) <- new_rdd) {
        file.write("[")
        file.write("\""+k+"\"")
        file.write("," + v)
        i = i+1
        if (i == new_rdd.length+1) {
          file.write("]")
        } else {
          file.write("],")
        }
      }
      file.write("]")
      file.write("}")
      file.flush()

    }



//










  }


}
