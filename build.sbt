ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-log-analysis",
    version := "1.0",
    scalaVersion := "2.12.18",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.6",
      "org.apache.spark" %% "spark-sql" % "3.5.6",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),

    Compile / run / fork := true,

    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),

    Test / fork := true,

    Test / javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),

    Test / testOptions += Tests.Argument(
      TestFrameworks.ScalaTest,
      "-o"
    ),

    Test / testOptions += Tests.Argument(
      TestFrameworks.ScalaTest,
      "-u",
      "target/test-reports"
    )
  )
