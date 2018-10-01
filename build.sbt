name := "untitled4"

version := "0.1"

scalaVersion := "2.11.12"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "mysql" % "mysql-connector-java" % "5.1.12",
  "commons-net" % "commons-net" % "3.6",
  "com.github.seancfoley" % "ipaddress" % "4.2.0",
  "com.maxmind.geoip2" % "geoip2" % "2.12.0"
)