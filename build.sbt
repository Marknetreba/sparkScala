name := "untitled4"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "mysql" % "mysql-connector-java" % "5.1.12",
  "commons-net" % "commons-net" % "3.6"
)