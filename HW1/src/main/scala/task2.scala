import java.io.{File, PrintWriter}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.ListMap
import scala.io.Source
import java.io.FileWriter
import scala.collection.mutable.ListBuffer





object task2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("inf-553_task1")
      .getOrCreate()

    val file1 = args(0)
    val file2 = args(1)
    val output = args(2)
    val option = args(3)

    val n = args(4).toInt


    val sc = spark.sparkContext;

    val reviews = spark.read.json(file1).rdd

    var reviews1  = reviews.map(x => ((x.getAs[String]("business_id")),(x.getAs[String]("stars"))))

    val business = spark.read.json(file2).rdd

    var business1 = business.map(x => ((x.getAs[String]("business_id")),(x.getAs[String]("categories")))).filter( kv => (kv._2 != "" )).map(x=> x._2.trim foreach(x._2.split(",")))

    print(business1.take(2).toList)















  }
}