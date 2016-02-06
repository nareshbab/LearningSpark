name := "LearningSparkSQL"

version := "1.0"

scalaVersion := "2.10.5"


resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" ,
  "org.apache.spark" %% "spark-mllib" % "1.6.0" ,
  "org.apache.spark" %% "spark-sql" % "1.6.0" ,
  "spark.jobserver" %% "job-server-api" % "0.6.1" ,
  "com.databricks" %% "spark-avro" % "2.0.1" ,
  "com.databricks" %% "spark-csv" % "1.3.0" ,
  "net.liftweb" %% "lift-json" % "2.6"
)

assemblyJarName in assembly := "Stats.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
    