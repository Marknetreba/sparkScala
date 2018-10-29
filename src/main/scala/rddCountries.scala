import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.exception.AddressNotFoundException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object rddCountries {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("yarn-cluster").setAppName("CountingSheep"))
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

    //Top 10 Countries
    data.mapValues(_.toString.toInt).reduceByKey(_ + _).sortBy(i => i._2, ascending = false).take(10).foreach(println)
  }
}
