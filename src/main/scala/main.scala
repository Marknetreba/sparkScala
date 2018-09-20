import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object main {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("CountingSheep"))

    val session = SparkSession.builder().appName("CountingSheep").getOrCreate()

    val csvFormat = "com.databricks.spark.csv"
    val ordersPath = "orders.csv"
    val ipPath = "/Users/mnetreba/Downloads/ips.csv"
    val countryPath = "/Users/mnetreba/Downloads/countries.csv"

    // RDD from orders.csv
    val rdd = session.read
      .format(csvFormat)
      .option("header", value = false)
      .load(ordersPath)
      .rdd

    // RDD from ips.csv
    val rddIps = session.read
      .format(csvFormat)
      .option("header", value = true)
      .load(ipPath)
      .rdd

    // RDD from countries.csv
    val rddCountries = session.read
      .format(csvFormat)
      .option("header", value = true)
      .load(countryPath)
      .rdd


    // Most frequently appeared categories with RDD
    rdd.map(i => i(3)).map(i => (i,1)).reduceByKey(_+_).sortBy(_._2, ascending = false).take(10)

    // Most frequently appeared products with RDD
    rdd.map(i => i(0)).map(i => (i,1)).reduceByKey(_+_).sortBy(_._2, ascending = false).take(10)

    // Top 10 countries with RDD
    val ipData = rddIps.map(i => (i(0),i(1)))
    val countryData = rddCountries.map(i => (i(0),i(5)))
    val ordersData = rdd.map(i => (i(4),i(1)))

    ipData.join(ordersData).map(i => i._2)
      .join(countryData).map(i => i._2)
      .map(_.swap)
      .mapValues(_.toString).mapValues(_.toInt).reduceByKey(_ + _).sortBy(i => i._2, ascending = false).take(10)


    // DF from orders.csv
    val orders = session.read.csv(ordersPath)
      .toDF("product_name","product_price","purchase_date","product_category","network")

    // DF from ips.csv
    val ips = session.read.csv(ipPath)
      .toDF("network","geoname_id","","","","")
      .select("network", "geoname_id")

    // DF from countries.csv
    val countries = session.read.csv(countryPath)
      .toDF("geoname_id","","","","","country_name","")
      .select("geoname_id","country_name")

    // Top 10 countries with DF
    val topDF = ips.join(countries, Seq("geoname_id"))
      .join(orders.select("product_price","network"), Seq("network"))
      .select("product_price","country_name")
      .groupBy("country_name")
      .agg(Map("product_price"->"sum"))
      .orderBy(desc("sum(product_price)"))
      .withColumnRenamed("sum(product_price)", "product_price")

    // Most frequently appeared categories with DF
    val categoriesDF = orders.groupBy("product_category").count().sort(desc("count"))

    // Most frequently appeared products with DF
    val productsDF = orders.groupBy("product_name").count().sort(desc("count"))


    // Write to MySQL
    val prop = new Properties()
    prop.put("user", "retail_dba")
    prop.put("password", "cloudera")

    val url = "jdbc:mysql://localhost:3306/retail_db"

    topDF.write.mode("append").jdbc(url,"spark_countries",prop)
    categoriesDF.write.mode("append").jdbc(url,"spark_categories",prop)
    productsDF.write.mode("append").jdbc(url,"spark_products",prop)

  }
}
