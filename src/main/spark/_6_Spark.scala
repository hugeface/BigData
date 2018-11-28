import org.apache.spark.sql.{DataFrame, SparkSession}

object _6_Spark {
  val spark = SparkSession.builder().appName("Cross Feature").master("local[2]").getOrCreate()
  val prior = spark.sql("select * from hive.order_products_prior")
  val orders = spark.sql("select * from hive.orders")
  /**
    * user and product Feature: cross feature 交叉特征
    * 1. 统计user和对应product在多少个订单中出现（distinct order_id）
    * 2. 特定product具体在购物车中的出现位置的平均位置
    * 3. 最后一个订单id
    * 4. 用户对应product在所有这个用户购买产品量中的占比rate
    * */
  // 统计用户和对应商品在多少个订单中出现
  def ordCntUserCrossProd(prior:DataFrame, orders:DataFrame): Unit = {
    val result = orders.join(prior, "order_id")
      .selectExpr("user_id", "product_id", "order_id").distinct()
      .groupBy("user_id", "product_id").count()
      .withColumnRenamed("count", "order_cnt").limit(10)
    result.show()
  }
  // 用户的商品在购物车中的出现位置的平均位置
  def avgPosition(prior:DataFrame, orders:DataFrame): Unit = {
    val result = orders.join(prior, "order_id")
      .selectExpr("user_id", "product_id", "cast(add_to_cart_order as INT) as position")
      .groupBy("user_id", "product_id").avg("position")
      .withColumnRenamed("avg(position)", "avg_order").limit(10)
    result.show()
  }
  // 用户每个订单中最后一个被添加的商品
  def lastOrder(prior:DataFrame, orders:DataFrame): Unit = {
    val result = orders.join(prior, "order_id").selectExpr("user_id", "order_id", "product_id", "add_to_cast_order as add_order").groupBy("user_id")
  }
  // 用户对应product在所有这个用户购买产品量中的占比
  def prodRate(prior:DataFrame, orders:DataFrame): Unit = {
    val tinyTable = prior.selectExpr("order_id", "product_id").join(orders.selectExpr("order_id", "user_id"), "order_id")
    val totalPrdCnt = tinyTable.groupBy("user_id").count().withColumnRenamed("count", "prod_cnt")
    val prdCnt = tinyTable.groupBy("user_id", "product_id").count().distinct().withColumnRenamed("count", "per_cnt_cnt")
    import prdCnt.sparkSession.implicits._
    val result = prdCnt.join(totalPrdCnt, "user_id").rdd.map(x => (x(0).toString, x(1).toString, x(2).toString.toDouble / x(3).toString.toDouble)).toDF("user_id", "product_id", "prod_rate").limit(10)
    result.show()
  }
}
