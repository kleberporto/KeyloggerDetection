name := "KeyloggerDetection"

version := "0.1"

scalaVersion := "2.13.7"

idePackagePrefix := Some("com.")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.apache.spark" %% "spark-ml" % "3.0.0"
)