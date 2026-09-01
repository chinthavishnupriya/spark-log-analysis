import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object LogAnalysis {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // 1. SPARK SESSION
    // --------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Spark Log Analysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("===== SPARK LOG ANALYSIS STARTED =====")


    // --------------------------------------------------
    // 2. READ RAW LOG FILE
    // --------------------------------------------------

    val rawDF = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("data/application.log")

    println("\n===== RAW LOG DATA =====")
    rawDF.show(false)


    // --------------------------------------------------
    // 3. ASSIGN COLUMN NAMES
    // --------------------------------------------------

    val logDF = rawDF.toDF(
      "timestamp",
      "level",
      "ip",
      "url",
      "status",
      "response_time"
    )

    println("\n===== LOG DATA WITH COLUMN NAMES =====")
    logDF.show(false)


    // --------------------------------------------------
    // 4. CONVERT DATA TYPES
    // --------------------------------------------------

    val typedLogDF = logDF
      .withColumn(
        "status",
        col("status").cast(IntegerType)
      )
      .withColumn(
        "response_time",
        col("response_time").cast(IntegerType)
      )

    println("\n===== TYPED LOG DATA =====")
    typedLogDF.show(false)


    // --------------------------------------------------
    // 5. DISPLAY SCHEMA
    // --------------------------------------------------

    println("\n===== LOG SCHEMA =====")
    typedLogDF.printSchema()


    // --------------------------------------------------
    // 6. DATA QUALITY ANALYSIS
    // --------------------------------------------------

    println("\n===== DATA QUALITY ANALYSIS =====")

    val dataQualityTotal = typedLogDF.count()

    println(
      s"Total Records Checked: $dataQualityTotal"
    )


    // NULL VALUE CHECK

    println("\n----- NULL VALUE CHECK -----")

    val nullCount = typedLogDF.select(
      sum(
        when(col("timestamp").isNull, 1)
          .otherwise(0)
      ).alias("timestamp_nulls"),

      sum(
        when(col("level").isNull, 1)
          .otherwise(0)
      ).alias("level_nulls"),

      sum(
        when(col("ip").isNull, 1)
          .otherwise(0)
      ).alias("ip_nulls"),

      sum(
        when(col("url").isNull, 1)
          .otherwise(0)
      ).alias("url_nulls"),

      sum(
        when(col("status").isNull, 1)
          .otherwise(0)
      ).alias("status_nulls"),

      sum(
        when(col("response_time").isNull, 1)
          .otherwise(0)
      ).alias("response_time_nulls")
    )

    nullCount.show(false)


    // DUPLICATE RECORD CHECK

    val distinctRecordCount =
      typedLogDF.distinct().count()

    val duplicateCount =
      dataQualityTotal - distinctRecordCount

    println(
      s"Duplicate Records: $duplicateCount"
    )


    // INVALID HTTP STATUS CHECK

    val invalidStatusCount = typedLogDF
      .filter(
        col("status").isNull ||
        col("status") < 100 ||
        col("status") > 599
      )
      .count()

    println(
      s"Invalid HTTP Status Records: $invalidStatusCount"
    )


    // INVALID RESPONSE TIME CHECK

    val invalidResponseTimeCount = typedLogDF
      .filter(
        col("response_time").isNull ||
        col("response_time") < 0
      )
      .count()

    println(
      s"Invalid Response Time Records: $invalidResponseTimeCount"
    )


    // REQUIRED FIELD CHECK

    val invalidRequiredFieldCount = typedLogDF
      .filter(
        col("timestamp").isNull ||
        col("level").isNull ||
        col("ip").isNull ||
        col("url").isNull
      )
      .count()

    println(
      s"Missing Required Field Records: $invalidRequiredFieldCount"
    )


    // VALID RECORD CHECK

    val validRecordCount = typedLogDF
      .filter(
        col("timestamp").isNotNull &&
        col("level").isNotNull &&
        col("ip").isNotNull &&
        col("url").isNotNull &&
        col("status").isNotNull &&
        col("status") >= 100 &&
        col("status") <= 599 &&
        col("response_time").isNotNull &&
        col("response_time") >= 0
      )
      .count()

    val invalidRecordCount =
      dataQualityTotal - validRecordCount

    println(
      s"Valid Records: $validRecordCount"
    )

    println(
      s"Invalid Records: $invalidRecordCount"
    )


    // --------------------------------------------------
    // 7. TOTAL REQUESTS
    // --------------------------------------------------

    println("\n===== TOTAL REQUESTS =====")

    val totalRequests = typedLogDF.count()

    println(
      s"Total Requests: $totalRequests"
    )


    // --------------------------------------------------
    // 8. LOG LEVEL ANALYSIS
    // --------------------------------------------------

    println("\n===== LOG LEVEL ANALYSIS =====")

    val levelAnalysis = typedLogDF
      .groupBy("level")
      .count()
      .orderBy("level")

    levelAnalysis.show(false)


    // --------------------------------------------------
    // 9. HTTP STATUS ANALYSIS
    // --------------------------------------------------

    println("\n===== HTTP STATUS ANALYSIS =====")

    val statusAnalysis = typedLogDF
      .groupBy("status")
      .count()
      .orderBy("status")

    statusAnalysis.show(false)


    // --------------------------------------------------
    // 10. ERROR LOG ANALYSIS
    // --------------------------------------------------

    println("\n===== ERROR LOGS =====")

    val errorLogs = typedLogDF
      .filter(
        col("level") === "ERROR"
      )

    errorLogs.show(false)

    val totalErrorLogs =
      errorLogs.count()

    println(
      s"Total Error Logs: $totalErrorLogs"
    )


    // --------------------------------------------------
    // 11. UNIQUE IP ADDRESSES
    // --------------------------------------------------

    println("\n===== UNIQUE IP ADDRESSES =====")

    val uniqueIPs = typedLogDF
      .select("ip")
      .distinct()
      .orderBy("ip")

    uniqueIPs.show(false)

    val uniqueIPCount =
      uniqueIPs.count()

    println(
      s"Unique IP Count: $uniqueIPCount"
    )


    // --------------------------------------------------
    // 12. BROADCAST VARIABLE
    // --------------------------------------------------

    val slowRequestThreshold = 400

    val broadcastThreshold =
      spark.sparkContext.broadcast(
        slowRequestThreshold
      )

    println("\n===== BROADCAST VARIABLE =====")

    println(
      s"Broadcasted Slow Request Threshold: ${broadcastThreshold.value} ms"
    )


    // --------------------------------------------------
    // 13. URL ANALYSIS
    // --------------------------------------------------

    println("\n===== URL ANALYSIS =====")

    val urlAnalysis = typedLogDF
      .groupBy("url")
      .count()
      .orderBy(desc("count"))

    urlAnalysis.show(false)


    // --------------------------------------------------
    // 14. URL + STATUS ANALYSIS
    // --------------------------------------------------

    println("\n===== URL + STATUS ANALYSIS =====")

    val urlStatusAnalysis = typedLogDF
      .groupBy("url", "status")
      .count()
      .orderBy("url", "status")

    urlStatusAnalysis.show(false)


    // --------------------------------------------------
    // 15. RESPONSE TIME STATISTICS
    // --------------------------------------------------

    println("\n===== RESPONSE TIME STATISTICS =====")

    val responseTimeStats = typedLogDF
      .agg(
        round(
          avg("response_time"),
          2
        ).alias("average_response_time"),

        min("response_time")
          .alias("minimum_response_time"),

        max("response_time")
          .alias("maximum_response_time")
      )

    responseTimeStats.show(false)


    // --------------------------------------------------
    // 16. RESPONSE TIME BY URL
    // --------------------------------------------------

    println("\n===== RESPONSE TIME BY URL =====")

    val responseTimeByURL = typedLogDF
      .groupBy("url")
      .agg(
        round(
          avg("response_time"),
          2
        ).alias("average_response_time"),

        min("response_time")
          .alias("minimum_response_time"),

        max("response_time")
          .alias("maximum_response_time")
      )
      .orderBy(
        desc("average_response_time")
      )

    responseTimeByURL.show(false)


    // --------------------------------------------------
    // 17. SLOW REQUESTS
    // --------------------------------------------------

    println("\n===== SLOW REQUESTS =====")

    val slowRequests = typedLogDF
      .filter(
        col("response_time") >=
          broadcastThreshold.value
      )
      .orderBy(
        desc("response_time")
      )

    slowRequests.show(false)

    val slowRequestCount =
      slowRequests.count()

    println(
      s"Slow Request Count: $slowRequestCount"
    )


    // --------------------------------------------------
    // 18. IP-WISE REQUEST ANALYSIS
    // --------------------------------------------------

    println(
      "\n===== IP-WISE REQUEST ANALYSIS ====="
    )

    val ipRequestAnalysis = typedLogDF
      .groupBy("ip")
      .agg(
        count("*")
          .alias("total_requests"),

        round(
          avg("response_time"),
          2
        ).alias("average_response_time"),

        min("response_time")
          .alias("minimum_response_time"),

        max("response_time")
          .alias("maximum_response_time")
      )
      .orderBy(
        desc("total_requests")
      )

    ipRequestAnalysis.show(false)


    // --------------------------------------------------
    // 19. IP-WISE ERROR ANALYSIS
    // --------------------------------------------------

    println(
      "\n===== IP-WISE ERROR ANALYSIS ====="
    )

    val ipErrorAnalysis = typedLogDF
      .groupBy("ip")
      .agg(
        count("*")
          .alias("total_requests"),

        sum(
          when(
            col("level") === "ERROR",
            1
          ).otherwise(0)
        ).alias("error_count")
      )
      .withColumn(
        "error_rate",
        round(
          col("error_count") * 100.0 /
            col("total_requests"),
          2
        )
      )
      .orderBy(
        desc("error_rate")
      )

    ipErrorAnalysis.show(false)


    // --------------------------------------------------
    // 20. TOP REQUESTING IP
    // --------------------------------------------------

    println(
      "\n===== TOP REQUESTING IP ====="
    )

    val topIP = typedLogDF
      .groupBy("ip")
      .count()
      .orderBy(
        desc("count")
      )
      .limit(1)

    topIP.show(false)


    // --------------------------------------------------
    // 21. IP WITH MOST ERRORS
    // --------------------------------------------------

    println(
      "\n===== IP WITH MOST ERRORS ====="
    )

    val topErrorIP = typedLogDF
      .filter(
        col("level") === "ERROR"
      )
      .groupBy("ip")
      .count()
      .orderBy(
        desc("count")
      )
      .limit(1)

    topErrorIP.show(false)


    // --------------------------------------------------
    // 22. ACCUMULATORS
    // --------------------------------------------------

    val totalRequestsAccumulator =
      spark.sparkContext.longAccumulator(
        "Total Requests"
      )

    val successfulRequestsAccumulator =
      spark.sparkContext.longAccumulator(
        "Successful Requests"
      )

    val errorRequestsAccumulator =
      spark.sparkContext.longAccumulator(
        "Error Requests"
      )

    val slowRequestsAccumulator =
      spark.sparkContext.longAccumulator(
        "Slow Requests"
      )


    // --------------------------------------------------
    // 23. PROCESS LOGS USING ACCUMULATORS
    // --------------------------------------------------

    typedLogDF.foreach { row =>

      totalRequestsAccumulator.add(1)

      val level =
        row.getAs[String]("level")

      val status =
        row.getAs[Int]("status")

      val responseTime =
        row.getAs[Int]("response_time")


      if (
        status >= 200 &&
        status < 400
      ) {
        successfulRequestsAccumulator.add(1)
      }


      if (level == "ERROR") {
        errorRequestsAccumulator.add(1)
      }


      if (
        responseTime >=
          broadcastThreshold.value
      ) {
        slowRequestsAccumulator.add(1)
      }
    }


    // --------------------------------------------------
    // 24. SPARK SQL ANALYSIS
    // --------------------------------------------------

    println(
      "\n===== SPARK SQL ANALYSIS ====="
    )

    typedLogDF.createOrReplaceTempView(
      "application_logs"
    )


    // --------------------------------------------------
    // SQL 1: ERROR LOGS
    // --------------------------------------------------

    println(
      "\n===== SQL ERROR LOGS ====="
    )

    val sqlErrors = spark.sql(
      """
        SELECT
          timestamp,
          level,
          ip,
          url,
          status,
          response_time
        FROM application_logs
        WHERE level = 'ERROR'
        ORDER BY timestamp
      """
    )

    sqlErrors.show(false)


    // --------------------------------------------------
    // SQL 2: REQUEST COUNT BY URL
    // --------------------------------------------------

    println(
      "\n===== SQL REQUEST COUNT BY URL ====="
    )

    val sqlURLCount = spark.sql(
      """
        SELECT
          url,
          COUNT(*) AS total_requests
        FROM application_logs
        GROUP BY url
        ORDER BY total_requests DESC
      """
    )

    sqlURLCount.show(false)


    // --------------------------------------------------
    // SQL 3: AVERAGE RESPONSE TIME BY URL
    // --------------------------------------------------

    println(
      "\n===== SQL AVERAGE RESPONSE TIME BY URL ====="
    )

    val sqlAverageResponse = spark.sql(
      """
        SELECT
          url,
          ROUND(
            AVG(response_time),
            2
          ) AS average_response_time
        FROM application_logs
        GROUP BY url
        ORDER BY average_response_time DESC
      """
    )

    sqlAverageResponse.show(false)


    // --------------------------------------------------
    // SQL 4: HTTP STATUS ANALYSIS
    // --------------------------------------------------

    println(
      "\n===== SQL HTTP STATUS ANALYSIS ====="
    )

    val sqlStatusAnalysis = spark.sql(
      """
        SELECT
          status,
          COUNT(*) AS total_requests
        FROM application_logs
        GROUP BY status
        ORDER BY status
      """
    )

    sqlStatusAnalysis.show(false)


    // --------------------------------------------------
    // SQL 5: IP-WISE REQUEST ANALYSIS
    // --------------------------------------------------

    println(
      "\n===== SQL IP-WISE REQUEST ANALYSIS ====="
    )

    val sqlIPAnalysis = spark.sql(
      """
        SELECT
          ip,
          COUNT(*) AS total_requests,
          ROUND(
            AVG(response_time),
            2
          ) AS average_response_time
        FROM application_logs
        GROUP BY ip
        ORDER BY total_requests DESC
      """
    )

    sqlIPAnalysis.show(false)


    // --------------------------------------------------
    // SQL 6: SLOW REQUESTS
    // --------------------------------------------------

    println(
      "\n===== SQL SLOW REQUESTS ====="
    )

    val sqlSlowRequests = spark.sql(
      s"""
        SELECT
          timestamp,
          ip,
          url,
          status,
          response_time
        FROM application_logs
        WHERE response_time >=
          ${broadcastThreshold.value}
        ORDER BY response_time DESC
      """
    )

    sqlSlowRequests.show(false)


    // --------------------------------------------------
    // 25. ACCUMULATOR SUMMARY
    // --------------------------------------------------

    println(
      "\n===== ACCUMULATOR SUMMARY ====="
    )

    println(
      s"Total Requests       : ${totalRequestsAccumulator.value}"
    )

    println(
      s"Successful Requests  : ${successfulRequestsAccumulator.value}"
    )

    println(
      s"Error Requests       : ${errorRequestsAccumulator.value}"
    )

    println(
      s"Slow Requests        : ${slowRequestsAccumulator.value}"
    )


    // --------------------------------------------------
    // 26. DATA QUALITY SUMMARY
    // --------------------------------------------------

    println(
      "\n===== DATA QUALITY SUMMARY ====="
    )

    println(
      s"Records Checked      : $dataQualityTotal"
    )

    println(
      s"Valid Records        : $validRecordCount"
    )

    println(
      s"Invalid Records      : $invalidRecordCount"
    )

    println(
      s"Duplicate Records    : $duplicateCount"
    )

    println(
      s"Invalid Status       : $invalidStatusCount"
    )

    println(
      s"Invalid Response Time: $invalidResponseTimeCount"
    )

    println(
      s"Missing Required Data: $invalidRequiredFieldCount"
    )


    // --------------------------------------------------
    // 27. FINAL PROJECT SUMMARY
    // --------------------------------------------------

    println(
      "\n===== FINAL PROJECT SUMMARY ====="
    )

    println(
      s"Total Requests       : $totalRequests"
    )

    println(
      s"Unique IP Addresses  : $uniqueIPCount"
    )

    println(
      s"Total Error Logs     : $totalErrorLogs"
    )

    println(
      s"Slow Requests        : $slowRequestCount"
    )

    println(
      s"Slow Threshold       : ${broadcastThreshold.value} ms"
    )

    println(
      s"Valid Records        : $validRecordCount"
    )

    println(
      s"Invalid Records      : $invalidRecordCount"
    )


    // --------------------------------------------------
    // 28. SAVE PROCESSED LOG DATA
    // --------------------------------------------------

    println(
      "\n===== SAVING OUTPUT FILES ====="
    )

    typedLogDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(
        "output/processed-logs"
      )


    // --------------------------------------------------
    // 29. SAVE ERROR LOGS
    // --------------------------------------------------

    errorLogs
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(
        "output/error-logs"
      )


    // --------------------------------------------------
    // 30. SAVE URL ANALYSIS
    // --------------------------------------------------

    urlAnalysis
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(
        "output/url-analysis"
      )


    // --------------------------------------------------
    // 31. SAVE STATUS ANALYSIS
    // --------------------------------------------------

    statusAnalysis
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(
        "output/status-analysis"
      )


    // --------------------------------------------------
    // 32. SAVE IP ANALYSIS
    // --------------------------------------------------

    ipRequestAnalysis
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(
        "output/ip-analysis"
      )


    // --------------------------------------------------
    // 33. SAVE SLOW REQUESTS
    // --------------------------------------------------

    slowRequests
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(
        "output/slow-requests"
      )


    // --------------------------------------------------
    // 34. PROJECT COMPLETED
    // --------------------------------------------------

    println(
      "Analysis outputs saved successfully."
    )

    println(
      "\n===== LOG ANALYSIS COMPLETED ====="
    )

    spark.stop()
  }
}
