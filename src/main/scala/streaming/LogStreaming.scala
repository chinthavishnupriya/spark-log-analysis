import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Spark Log Streaming Analysis")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    println("===== SPARK STREAMING ANALYSIS STARTED =====")

    val lines = ssc.socketTextStream("localhost", 9999)

    var totalProcessedLogs = 0L

    lines.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val logs = rdd.map { line =>
          val parts = line.split(",")

          (
            parts(0), // timestamp
            parts(1), // level
            parts(2), // IP
            parts(3), // URL
            parts(4).toInt, // status
            parts(5).toInt // response time
          )
        }

        val totalLogs = logs.count()

        totalProcessedLogs += totalLogs

        val infoCount =
          logs.filter(_._2 == "INFO").count()

        val errorCount =
          logs.filter(_._2 == "ERROR").count()

        val slowRequests =
          logs.filter(_._6 > 500).count()

        val statusCounts = logs
          .map(log => log._5)
          .map(status => (status, 1))
          .reduceByKey(_ + _)
          .collect()

        val ipCounts = logs
          .map(log => log._3)
          .map(ip => (ip, 1))
          .reduceByKey(_ + _)
          .collect()

        val urlCounts = logs
          .map(log => log._4)
          .map(url => (url, 1))
          .reduceByKey(_ + _)
          .collect()

        println("\n========================================")
        println("        STREAMING LOG ANALYSIS")
        println("========================================")

        println(s"Total Logs       : $totalLogs")
        println(s"INFO Logs        : $infoCount")
        println(s"ERROR Logs       : $errorCount")
        println(s"Slow Requests    : $slowRequests")

        println("\n--- Status Code Analysis ---")

        statusCounts.foreach {
          case (status, count) =>
            println(s"$status -> $count")
        }

        println("\n--- IP Address Analysis ---")

        ipCounts.foreach {
          case (ip, count) =>
            println(s"$ip -> $count")
        }

        println("\n--- URL Analysis ---")

        urlCounts.foreach {
          case (url, count) =>
            println(s"$url -> $count")
        }

        println("========================================\n")

        println(s"Total records processed so far: $totalProcessedLogs")

        if (totalProcessedLogs >= 10) {
          println("\n===== ALL EXPECTED LOGS PROCESSED =====")
          println("===== STOPPING SPARK STREAMING =====")

          ssc.stop(
            stopSparkContext = true,
            stopGracefully = true
          )
        }
      }
    }

    ssc.start()

    println("Streaming context started. Waiting for log data...")

    ssc.awaitTermination()

    println("===== SPARK STREAMING ANALYSIS FINISHED =====")
  }
}
