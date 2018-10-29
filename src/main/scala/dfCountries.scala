import java.net.InetAddress
import java.util.Properties

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.exception.AddressNotFoundException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.desc
import org.apache.spark.{SparkConf, SparkContext}

object dfCountries {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("yarn-cluster").setAppName("CountingSheep"))
    val sql = new SQLContext(sc)

    // MySQL configs
    val prop = new Properties()
    prop.put("user", "")
    prop.put("password", "")
    val url = "jdbc:mysql://ip-10-0-0-21.us-west-1.compute.internal:3306/retail_db"

    val ordersPath = "hdfs:///tmp/orders/orders.csv"

    // RDD from orders.csv
    val rddOrders = sc.textFile(ordersPath).map(line => line.split(",").map(elem => elem.trim))

    // Converting map with valid ips to RDD
    val ipData = rddOrders.map(i => (i(4), i(1)))

    val data = ipData.mapPartitions(part => {
      val fs = FileSystem.get(new Configuration())
      val in = fs.open(new Path("/tmp/countries.mmdb"))
      val reader = new DatabaseReader.Builder(in).build()
      part.map(p => (p._2, {
        val ipAddress = InetAddress.getByName(p._1.toString)
        try {
          val response = reader.country(ipAddress)
          response.getCountry.getNames.get("en")
        }
        catch {
          case e: AddressNotFoundException => ""
        }
      }))
    }).map(_.swap).filter(i => i._1 != "")

    // Top 10 countries with DF
    val dfValidIps = sql.createDataFrame(data.filter(i => i._1 != null && i._2 != null).map(i => (i._1.toString, i._2.toString))).toDF("country", "product_price")

    val topDF = dfValidIps.groupBy("country")
      .agg(Map("product_price" -> "sum"))
      .orderBy(desc("sum(product_price)"))
      .withColumnRenamed("sum(product_price)", "product_price")
      .limit(10)

    topDF.show()

    // Write to MySQL
    topDF.write.mode("append").jdbc(url, "spark_countries", prop)
  }
}
