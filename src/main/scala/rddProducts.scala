import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class rddProducts {
    def go(): Unit = {
      val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("CountingSheep"))
      val sql = new SQLContext(sc)
      val csvFormat = "com.databricks.spark.csv"
      val ordersPath = "orders.csv"

      // RDD from orders.csv
      val rddOrders = sql.read
        .format(csvFormat)
        .option("header", value = false)
        .load(ordersPath)
        .rdd

      // Most frequently appeared products with RDD
      rddOrders.map(i => i(0)).map(i => (i, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(10)

    }
}
