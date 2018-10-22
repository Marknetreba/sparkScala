import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.desc
import org.apache.spark.{SparkConf, SparkContext}

class dfCategories {
    def job(): Unit = {
        val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("CountingSheep"))
        val sql = new SQLContext(sc)

        // MySQL configs
        val prop = new Properties()
        prop.put("user", "retail_dba")
        prop.put("password", "cloudera")
        val url = "jdbc:mysql://localhost:3306/retail_db"

        val ordersPath = "orders.csv"

        // DF from orders.csv
        val orders = sql.read.csv(ordersPath)
          .toDF("product_name", "product_price", "purchase_date", "product_category", "client_ip")

        // Most frequently appeared categories with DF
        val categoriesDF = orders.groupBy("product_category").count().sort(desc("count")).limit(10)

        //Write to MySQL
        categoriesDF.write.mode("append").jdbc(url, "spark_categories", prop)
    }
}