import java.util.Properties

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object dfCategories {

    def main(args: Array[String]){
        val sc = new SparkContext(new SparkConf().setMaster("yarn-cluster").setAppName("CountingSheep"))
        val sql = new SQLContext(sc)

        // MySQL configs
        val prop = new Properties()
        prop.put("user", "")
        prop.put("password", "")
        val url = "jdbc:mysql://ip-10-0-0-21.us-west-1.compute.internal:3306/retail_db"

        val ordersPath = "hdfs:///tmp/orders/orders.csv"

        // DF from orders.csv
        val orders = sc.textFile(ordersPath).map(line => line.split(",")
          .map(elem => elem.trim))
          .map(row => Row(row(0),row(1),row(2),row(3),row(4)))

        val schema = new StructType(Array(StructField("product_name", StringType, true),
          StructField("product_price", StringType, true),
          StructField("purchase_date", StringType, true),
          StructField("product_category",StringType, true),
          StructField("client_ip",StringType, true)))

        val df = sql.createDataFrame(orders, schema)

        // Most frequently appeared categories with DF
        val categoriesDF = df.groupBy("product_category").count().sort(desc("count")).limit(10)

        categoriesDF.show()

        //Write to MySQL
        categoriesDF.write.mode("append").jdbc(url, "spark_categories", prop)
    }
}
