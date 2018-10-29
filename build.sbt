name := "untitled4"

version := "0.1"

scalaVersion := "2.10.6"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "mysql" % "mysql-connector-java" % "5.1.12",
  "com.maxmind.geoip2" % "geoip2" % "2.5.0"
)

assemblyJarName in assembly := "spark.jar"