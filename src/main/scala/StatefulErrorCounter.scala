import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulErrorCounter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("StatefulErrorCounter")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999)

    val errors = lines
      .filter(_.contains("ERROR"))
      .map(_ => ("ERROR", 1))

    val updateFunction = (
      values: Seq[Int],
      state: Option[Int]
    ) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val runningErrors =
      errors.updateStateByKey(updateFunction)

    runningErrors.print()

    ssc.start()

    println("===== STATEFUL ERROR COUNTER STARTED =====")
    println("Waiting for data on localhost:9999...")

    ssc.awaitTermination()
  }
}
