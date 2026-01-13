name := "SparkScalaDemo"

version := "0.1"

scalaVersion := "2.12.17"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  // 1. Core Spark Libraries (The Engine)
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  
  // 2. Kafka Connector (Needed for readStream.format("kafka"))
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // 3. Postgres JDBC Driver (Needed for the Sink)
  "org.postgresql" % "postgresql" % "42.6.0"
)