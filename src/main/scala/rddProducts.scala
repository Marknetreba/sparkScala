import org.apache.spark.{SparkConf, SparkContext}

object rddProducts {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("yarn-cluster").setAppName("CountingSheep"))
    val ordersPath = "hdfs:///tmp/orders/orders.csv"

    // RDD from orders.csv
    val rddOrders = sc.textFile(ordersPath).map(line => line.split(",").map(elem => elem.trim))

    // Most frequently appeared products with RDD
    rddOrders.map(i => i(0)).map(i => (i, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(10).foreach(println)
  }
}
