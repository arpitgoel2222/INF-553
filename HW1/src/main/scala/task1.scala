import java.io.{File, PrintWriter}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.ListMap
import scala.io.Source
import java.io.FileWriter
import scala.collection.mutable.ListBuffer




object task1 {
  val punctuations = List('(',')','[',']',',','.','!','?',':',';')
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("inf-553_task1")
      .getOrCreate()

    val input_file = args(0)
    val output_file = args(1)
    val stopwords = args(2)
    val y = args(3)
    val m = args(4).toInt
    val n = args(5).toInt



    val sc = spark.sparkContext;

    val data = spark.read.json(input_file).rdd

    // Useful reviews
    val total_reviews = data.map(x => (x.getAs[String]("review_id"))).count()
//    println(total_reviews)

    val year_reviews = data.map(x => (x.getAs[String]("date"))).filter(x => x.contains(y)).count()
//    println(year_reviews)

    val distinct_users =  data.map(x => (x.getAs[String]("user_id"))).distinct().count()
//    println(distinct_users)

    val top_m_users = data.map(x => (x.getAs[String]("user_id"),1)).reduceByKey((x,y) => x+y).sortBy(x => (-x._2, x._1)).take(m).toList
//    println(top_m_users.toList)

    var words_to_remove = ListBuffer[String]()
    for (line <- Source.fromFile(stopwords).getLines) {
      val lines  = line.trim
      words_to_remove+=lines
    }
//    println(words_to_remove)
    val slist = words_to_remove.toList
//    println(slist)



    val words = data.flatMap(x => (x.getAs[String]("text")).toLowerCase.trim.split(" ")).map(x => x.replaceAll("\\p{Punct}", "")).filter(!slist.contains(_)).filter(x=> x!="").map(x => (x,1)).reduceByKey((x,y) => x+y).sortBy(x => (-x._2, x._1)).keys.take(n).toList
//    println(words.keys.take(10).toList)
//    words.map(x => (x.getAs[String]("user_id"),1)).reduceByKey((x,y) => x+y).sortBy(x => (-x._2, x._1)).take(10)
//    println(words.toList)


    val file = new FileWriter(output_file)
    file.write("{")

    file.write("\"A\": " + total_reviews + ",")

    file.write("\"B\": " + year_reviews + ",")

    file.write("\"C\": " + distinct_users + ",")

    file.write("\"D\": [")
    var i = 1
    for ((k, v) <- top_m_users) {
      file.write("[")
      file.write("\""+k+"\"")
      file.write("," + v)
      i = i+1
      if (i == m+1) {
        file.write("]")
      } else {
        file.write("],")
      }
    }
    file.write("],")

    file.write("\"E\": [")
    var j=1
    for (x <- words) {

      file.write("\""+x+"\"")
      j=j+1
      if(j==n+1)
        {
          file.write("]")
        }
      else{
        file.write(",")
      }

    }
    file.write("}")
    file.flush()


























  }

  def removep(word:String) :String={
    val new_word2 = new StringBuilder("")
    val word = " Arpit [Goel "
    val new_word = word.trim()
//    val punctuations = List('(',')','[',']',',','.','!','?',':',';')
    print(new_word)
    for(c <- word)
    {
      if(!punctuations.contains(c))
      {
        new_word2.append(c)
      }
    }
    return(new_word2.toString())

  }


}
