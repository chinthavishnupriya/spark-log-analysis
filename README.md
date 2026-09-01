# Spark Log Analysis

A Apache Spark project built using Scala to analyze web application log data.

## Project Overview

This project demonstrates how Apache Spark can be used to process and analyze application logs.

The input data contains:

- Timestamp
- Log level
- IP address
- URL
- HTTP status code
- Response time

The project performs different types of analysis including request counting, error analysis, URL analysis, response-time analysis, IP-wise analysis, Broadcast Variables, and Accumulators.

## Technologies Used

- Scala
- Apache Spark 3.5.6
- Spark SQL
- SBT
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
