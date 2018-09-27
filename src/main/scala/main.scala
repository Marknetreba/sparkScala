import java.sql.DriverManager
import java.util.Properties

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object main {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("CountingSheep"))

    val session = SparkSession.builder().appName("CountingSheep").getOrCreate()
    import session.implicits._

    // MySQL configs
    val prop = new Properties()
    prop.put("user", "retail_dba")
    prop.put("password", "cloudera")
    val url = "jdbc:mysql://localhost:3306/retail_db"

    val csvFormat = "com.databricks.spark.csv"
    val ordersPath = "/Users/mnetreba/Downloads/orders.csv"
    val ipPath = "/Users/mnetreba/Downloads/ips.csv"
    val countryPath = "/Users/mnetreba/Downloads/countries.csv"

    // RDD from orders.csv
    val rddOrders = session.read
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
    rddOrders.map(i => i(3)).map(i => (i,1)).reduceByKey(_+_).sortBy(_._2, ascending = false).take(10)

    // Most frequently appeared products with RDD
    rddOrders.map(i => i(0)).map(i => (i,1)).reduceByKey(_+_).sortBy(_._2, ascending = false).take(10)


    // Converting map with valid ips to RDD
    val ipsPartitions = rddOrders.map(i => (i(4),i(1))).map(i => (i._1.toString, i._2.toString)).coalesce(5)

    // All possible IP
    val ipData = rddIps.map(i => (i(1),i(0)))
    val countryData = rddCountries.map(i => (i(0),i(5))).filter(i => i._2 != null)

    val data = ipData.join(countryData).map(_._2)
      .map(i => (i._2, i._1, {
        val utils = new SubnetUtils(i._1.toString)
        utils.getInfo.getAllAddresses.toList
      })).map(i => ((i._1, i._2), i._3))
      .flatMapValues(x => x)
      .map(_.swap)
      .join(ipsPartitions)
      .coalesce(5)

    data.foreachPartition(part => {
      val conn = DriverManager.getConnection(url, "retail_dba", "cloudera")
      part.foreach(ip => {
        val utils = new SubnetUtils(ip._2._1._2.toString)
        if (utils.getInfo.isInRange(ip._1))
          conn.createStatement().execute("INSERT INTO ip_countries(ip, country) VALUES (" + ip + ","+ ip._2._1._1 +")" )
        else ""
      })
    })

    // Take data from MySQL
    val tableRDD = session.read.jdbc(url, "ip_countries", prop)
      .select("ip","country")
      .map(i => (i(0).toString, i(1).toString)).rdd

    // Top 10 countries with RDD
    val ordersData = rddOrders.map(i => (i(4),i(1))).map(i => (i._1.toString,i._2.toString)).coalesce(5)

    ordersData.join(tableRDD).map(i => i._2).map(_.swap)
      .map(_.swap)
      .mapValues(_.toInt).reduceByKey(_ + _)
      .sortBy(i => i._2, ascending = false).take(10)

    // DF from orders.csv
    val orders = session.read.csv(ordersPath)
      .toDF("product_name","product_price","purchase_date","product_category","client_ip")

    // DF from ips.csv
    val ips = session.read.csv(ipPath)
      .toDF("network","geoname_id","","","","")
      .select("network", "geoname_id")

    // DF from countries.csv
    val countries = session.read.csv(countryPath)
      .toDF("geoname_id","","","","","country_name","")
      .select("geoname_id","country_name")

    // Converting map with valid ips to DF
    val dfValidIps = session.read.jdbc(url, "ip_countries", prop).select("ip","country").toDF("client_ip", "country")

    // Top 10 countries with DF
    val topDF = dfValidIps.join(orders.select("product_price","client_ip"), Seq("client_ip"))
      .select("product_price","country")
      .groupBy("country")
      .agg(Map("product_price"->"sum"))
      .orderBy(desc("sum(product_price)"))
      .withColumnRenamed("sum(product_price)", "product_price")
      .limit(10)

    // Most frequently appeared categories with DF
    val categoriesDF = orders.groupBy("product_category").count().sort(desc("count")).limit(10)

    // Most frequently appeared products with DF
    val productsDF = orders.groupBy("product_name").count().sort(desc("count")).limit(10)


    // Write to MySQL

    topDF.write.mode("append").jdbc(url,"spark_countries",prop)
    categoriesDF.write.mode("append").jdbc(url,"spark_categories",prop)
    productsDF.write.mode("append").jdbc(url,"spark_products",prop)
  }
}
