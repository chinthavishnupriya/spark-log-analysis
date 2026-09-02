import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatelessWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Stateless Word Count")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()

    println("===== STATELESS WORD COUNT STARTED =====")
    println("Waiting for data on localhost:9999...")

    ssc.awaitTermination()
  }
}
