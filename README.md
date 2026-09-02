# Spark Log Analysis

A practical Apache Spark project built with Scala to analyze web application log data. The project demonstrates Spark DataFrame operations, Spark SQL, Broadcast Variables, Accumulators, data-quality validation, and CSV output generation.

## Project Overview

The application reads web application logs, converts the raw data into a typed Spark DataFrame, validates data quality, performs multiple analytical queries, and saves the results as CSV files.

### Input Fields

- `timestamp` — request timestamp
- `level` — log level such as `INFO` or `ERROR`
- `ip` — client IP address
- `url` — requested endpoint
- `status` — HTTP status code
- `response_time` — response time in milliseconds

## Architecture

```text
application.log
      |
      v
Spark CSV Reader
      |
      v
Raw DataFrame
      |
      v
Column Naming + Type Conversion
      |
      v
Data Quality Validation
      |
      +------------------------------+
      |                              |
      v                              v
DataFrame Analysis              Spark SQL Analysis
      |                              |
      +--------------+---------------+
                     |
                     v
          Broadcast + Accumulators
                     |
                     v
              CSV Output Files
```

## Data Quality Checks

The project validates the input before analysis by checking:

- Total records checked
- Null or missing values in required columns
- Duplicate records
- Invalid HTTP status codes
- Invalid response times
- Valid records
- Invalid records

For the current sample dataset, all 10 records are valid.

## Spark Features Demonstrated

### DataFrames

Used for structured log processing, filtering, grouping, aggregation, sorting, and output generation.

### Spark SQL

The DataFrame is registered as the `application_logs` temporary view and queried using SQL for error logs, URL request counts, average response times, HTTP status analysis, IP analysis, and slow requests.

### Broadcast Variable

A slow-request threshold of `400 ms` is broadcast to executors so the threshold can be reused during distributed processing.

### Accumulators

Accumulators are used to count:

- Total requests
- Successful requests
- Error requests
- Slow requests

## Analyses Performed

1. Total request count
2. Log-level analysis
3. HTTP status analysis
4. Error-log analysis
5. Unique IP analysis
6. URL request analysis
7. URL + HTTP status analysis
8. Response-time statistics
9. Response time by URL
10. Slow-request analysis
11. IP-wise request analysis
12. IP-wise error analysis
13. Top requesting IP
14. IP with the most errors
15. Spark SQL analysis
16. Data quality analysis
17. Accumulator summary

## Current Dataset Results

The sample dataset contains 10 application-log records.

| Metric | Result |
|---|---:|
| Total Requests | 10 |
| Successful Requests | 7 |
| Error Requests | 3 |
| Unique IP Addresses | 5 |
| Slow Requests | 3 |
| Slow Threshold | 400 ms |
| Valid Records | 10 |
| Invalid Records | 0 |
| Duplicate Records | 0 |
| Invalid HTTP Status Records | 0 |
| Invalid Response Time Records | 0 |

## Technologies Used

- Scala 2.12
- Apache Spark 3.5.6
- Spark SQL
- SBT
- Java 17
- Linux / WSL2
- Git
- GitHub

## Project Structure

```text
spark-log-analysis/
│
├── data/
│   └── application.log
│
├── output/
│   ├── error-logs/
│   │   └── error-logs.csv
│   ├── ip-analysis/
│   │   └── ip-analysis.csv
│   ├── processed-logs/
│   │   └── processed-logs.csv
│   ├── slow-requests/
│   │   └── slow-requests.csv
│   ├── status-analysis/
│   │   └── status-analysis.csv
│   ├── url-analysis/
│   │   └── url-analysis.csv
│   └── spark-log-analysis-output.txt
│
├── src/
│   └── main/
│       └── scala/
│           └── LogAnalysis.scala
│
├── build.sbt
├── project/
│   └── build.properties
├── .gitignore
└── README.md
```

## How to Run

### 1. Enter the project

```bash
cd ~/spark-log-analysis
```

### 2. Compile

```bash
sbt compile
```

### 3. Run

```bash
sbt run
```

To save the complete terminal output:

```bash
sbt run > output/spark-log-analysis-output.txt 2>&1
```

## Output Files

The application generates these analytical CSV datasets:

- `processed-logs.csv` — processed and typed log records
- `error-logs.csv` — error-level requests
- `url-analysis.csv` — request count by URL
- `status-analysis.csv` — request count by HTTP status
- `ip-analysis.csv` — IP-wise request statistics
- `slow-requests.csv` — requests meeting the 400 ms slow-request threshold
- `spark-log-analysis-output.txt` — complete application output

## Key Learning Outcomes

This project provides hands-on practice with:

- Spark application development using Scala
- DataFrame transformations and actions
- Schema and data-type handling
- Data-quality validation
- GroupBy and aggregation operations
- Spark SQL
- Broadcast Variables
- Accumulators
- Distributed-style log processing
- CSV result generation
- Building and running Spark applications with SBT
- Git and GitHub project management

## Sample Log Format

```text
2026-09-01 10:15:01,INFO,192.168.1.10,/home,200,120
2026-09-01 10:15:05,ERROR,192.168.1.12,/login,500,100
```

## Spark Streaming

The project also demonstrates real-time stream processing using Spark DStreams.

### Streaming Architecture

Data Producer -> TCP Socket -> localhost:9999 -> Spark StreamingContext -> 5-second batches -> DStream

DStream processing is divided into stateless and stateful processing.

### Streaming Applications

The project contains three Spark Streaming applications:

1. `StatelessWordCount.scala` — performs word counting independently for each micro-batch.
2. `StatefulWordCount.scala` — maintains running word counts across batches using `updateStateByKey`.
3. `StatefulErrorCounter.scala` — filters `ERROR` records and maintains a cumulative error count across batches.

All streaming applications use:

- Spark Streaming DStreams
- `StreamingContext`
- 5-second batch intervals
- TCP socket input on `localhost:9999`
- Local Spark execution with `local[*]`

### Stateful Processing

Stateful applications use checkpointing and `updateStateByKey` to maintain information across batches.

The checkpoint directory is excluded from Git because it contains generated runtime state.

### Streaming Test Results

The Stateful Word Count test verified cumulative state:

| Batch | Result |
|---|---|
| 1 | `(apple,2)`, `(orange,1)` |
| 2 | `(apple,3)`, `(orange,1)`, `(banana,1)` |
| 3 | `(apple,4)`, `(orange,2)`, `(banana,1)` |

The Stateful Error Counter test verified cumulative error tracking:

| Batch | Running ERROR Count |
|---|---:|
| 1 | 1 |
| 2 | 3 |
| 3 | 4 |

### Streaming Output Files

```text
output/
└── streaming/
    ├── streaming-log-analysis-output.txt
    ├── stateless/
    │   └── stateless-word-count-output.txt
    └── stateful/
        ├── stateful-word-count-output.txt
        └── stateful-error-counter-output.txt
```

### Running the Streaming Applications

Start a TCP producer in one terminal:

```bash
nc -lk 9999
```

Then run one of the applications in another terminal:

```bash
sbt "runMain StatelessWordCount"
```

```bash
sbt "runMain StatefulWordCount"
```

```bash
sbt "runMain StatefulErrorCounter"
```

## Status

**Project active and working.** The current implementation compiles successfully, runs successfully with Apache Spark 3.5.6, performs data-quality validation and analysis, and generates the documented output datasets.