import org.apache.spark.sql.SparkSession

object Lesson_5 {
  // 统计product被购买的次数
  def productBuyCnt(): Unit = {
    val spark = SparkSession.builder().appName("Calculate Product Buy Times ").master("yarn-cluster").getOrCreate()
    val df = spark.sql("select * from hive.order_products_prior")
    val prdBuyCnt = df.select("order_id", "product_id").groupBy("product_id").count().limit(10)
    prdBuyCnt.show()
  }
  // 统计product再次购买的数量
  def productRebuyCnt(): Unit = {
    val spark = SparkSession.builder().appName("Calculate Product Re-buy Times ").master("yarn-cluster").getOrCreate()
    val df = spark.sql("select * from hive.order_products_prior")
    val rebuyCnt = df.select("product_id", "reordered").where("reordered > 0" ).groupBy("product_id").agg("reordered" -> "sum").toDF("product_id", "reorder_cnt").limit(10)
    rebuyCnt.show()
  }
}
