import java.io.File
import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.exception.AddressNotFoundException
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object rddCountries {
  def main(args: Array[String]): Unit = {
    job()
  }

  def job(): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("spark://master:7077").setAppName("CountingSheep"))
    val sql = new SQLContext(sc)

    val csvFormat = "com.databricks.spark.csv"
    val ordersPath = "hdfs:///tmp/orders/orders.csv"
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
      }))
    }).map(_.swap).filter(i => i._1 != "")

    //Top 10 Countries
    data.mapValues(_.toString.toInt).reduceByKey(_ + _).sortBy(i => i._2, ascending = false).take(10)
  }
}
