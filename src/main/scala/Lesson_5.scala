import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Lesson_5 {
  /**
    * Product 统计、特征
    */
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
//    val rebuyCnt = df.select("product_id", "reordered").where("reordered > 0" ).groupBy("product_id").agg("reordered" -> "sum").toDF("product_id", "reorder_cnt").limit(10)
    val rebuyCnt = df.selectExpr("product_id", "case(reordered as int)")
      .groupBy("product_id")
      .agg(sum("reordered").as("prod_sum_rod"),
        avg("reordered").as("prod_rod_rate"),
        count("product_id").as("prod_cnt"))
    rebuyCnt.show()
  }
  // 统计product再次购买的比例
  def reorderRatio(): Unit = {
    val spark = SparkSession.builder().appName("Calculate Product Re-buy Ratio").master("yarn-cluster").getOrCreate()
    val df = spark.sql("select * from hive.order_products_prior")
    val prdBuyCnt = df.select("order_id", "product_id").groupBy("product_id").count()
    val rebuyCnt = df.select("product_id", "reordered").where("reordered > 0" ).groupBy("product_id").agg("reordered" -> "sum").toDF("product_id", "reorder_cnt")
    val joinTable = prdBuyCnt.join(rebuyCnt, "product_id")
    val reorderRatio = joinTable.map(x => (x(0).toString, (x(2).toString.toDouble / x(1).toString.toDouble).formatted("%.2f"))).toDF("product_id", "reorder_ratio").limit(10)
    reorderRatio.show()
  }

  /**
    * User 统计、特征
    */
  // 每个用户产生订单的平均间隔
  def avgInterval(): Unit ={
    val spark = SparkSession.builder().appName("Calculate Average Order Interval Of User").master("local[2]").getOrCreate()
  }
}
