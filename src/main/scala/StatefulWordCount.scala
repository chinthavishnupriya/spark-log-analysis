import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    // 1. Spark configuration
    val conf = new SparkConf()
      .setAppName("StatefulWordCount")
      .setMaster("local[*]")

    // 2. Create StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    // 3. Required for stateful operations
    ssc.checkpoint("checkpoint")

    // 4. Read data from producer
    val lines = ssc.socketTextStream(
      "localhost",
      9999
    )

    // 5. Split lines into words
    val words = lines.flatMap(
      line => line.split("\\s+")
    )

    // 6. Create (word, 1)
    val pairs = words.map(
      word => (word.toLowerCase, 1)
    )

    // 7. Stateful function
    val updateFunction =
      (values: Seq[Int], state: Option[Int]) => {

        val currentCount = values.sum
        val previousCount = state.getOrElse(0)

        Some(currentCount + previousCount)
      }

    // 8. Maintain state across batches
    val runningCounts =
      pairs.updateStateByKey(updateFunction)

    // 9. Print result
    runningCounts.print()

    // 10. Start streaming
    ssc.start()

    // 11. Wait
    ssc.awaitTermination()
  }
}
