import org.apache.spark.{SparkConf, SparkContext}

object rddProducts {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("yarn-cluster").setAppName("CountingSheep"))
    val ordersPath = "hdfs:///tmp/orders/orders.csv"

    // RDD from orders.csv
    val rddOrders = sc.textFile(ordersPath).map(line => line.split(",").map(elem => elem.trim))

    // Most frequently appeared products with RDD
    rddOrders.map(i => ((i(3), i(0)), i(1).toString.toInt))
      .reduceByKey(_+_)
      .map(i => (i._1._1,i._1._2,i._2))
      .sortBy(_._1, ascending = false)
      .foreach(println)
  }
}
