import java.io.File
import java.net.InetAddress
import java.util.Properties

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.exception.AddressNotFoundException
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.desc

class dfCountries {
    def job(): Unit = {
      val sc = new SparkContext(new SparkConf().setMaster("spark://master:7077").setAppName("CountingSheep"))
      val sql = new SQLContext(sc)

      // MySQL configs
      val prop = new Properties()
      prop.put("user", "retail_dba")
      prop.put("password", "cloudera")
      val url = "jdbc:mysql://localhost:3306/retail_db"

      val ordersPath = "hdfs:///tmp/orders/orders.csv"
      val csvFormat = "com.databricks.spark.csv"
      val mmdb = new File("/Users/mnetreba/Downloads/mmdb/countries.mmdb")

      // RDD from orders.csv
      val rddOrders = sql.read
        .format(csvFormat)
        .option("header", value = false)
        .load(ordersPath)
        .rdd

      // Converting map with valid ips to RDD
      val ipData = rddOrders.map(i => (i(4), i(1))).coalesce(5)

      val data = ipData.mapPartitions(part => {
        val reader = new DatabaseReader.Builder(mmdb).build()
        part.map(p => (p._2, {
          val ipAddress = InetAddress.getByName(p._1.toString)
          try {
            val response = reader.country(ipAddress)
            response.getCountry.getNames.get("en")
          }
          catch {
            case e: AddressNotFoundException => ""
          }
        }))}).map(_.swap).filter(i => i._1 != "")

      // Top 10 countries with DF
      val dfValidIps = sql.createDataFrame(data.filter(i => i._1 != null && i._2!=null).map(i => (i._1.toString, i._2.toString))).toDF("country", "product_price")

      val topDF = dfValidIps.groupBy("country")
        .agg(Map("product_price" -> "sum"))
        .orderBy(desc("sum(product_price)"))
        .withColumnRenamed("sum(product_price)", "product_price")
        .limit(10)

      // Write to MySQL
      topDF.write.mode("append").jdbc(url, "spark_countries", prop)
    }
}
