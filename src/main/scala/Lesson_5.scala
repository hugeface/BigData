import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spark.implicits._

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
    val orders = spark.sql("select * from hive.orders")
    val pureOrders = orders.selectExpr("*", "if(days_since_prior_order='', 0, days_since_prior_order) as dspo").drop("days_since_prior_order")
    val userGap = pureOrders.selectExpr("user_id", "case(dspo as int)").groupBy("user_id").avg("dspo").withColumnRenamed("avg(dspo)", "u_avg_day_gap").limit(10)
    userGap.show()
  }
  // 每个用户的总订单数
  def orderCnt(): Unit = {
    val spark = SparkSession.builder().appName("Calculate Order Count Of User").master("local[2]").getOrCreate()
    val orders = spark.sql("select * from hive.orders")
    val orderCnt = orders.groupBy("user_id").count().limit(10)
    orderCnt.show()
  }
  // 每个用户购买的商品种类
  def prodSet(): Unit = {
    val spark = SparkSession.builder().appName("Calculate Orders User Have Bought").master("local[2]").getOrCreate()
    val orders = spark.sql("select * from hive.orders")
    val priors = spark.sql("select * from hive.order_products_prior")
    val prodSet = orders.join(priors, "order_id").select("user_id", "product_id")
    // implicits作为对象存在于sparkSession类中，只有sparkSession被实例化后，才会在实例对象中被创建，所以要通过实例来引用
    import priors.sparkSession.implicits._
    val uniOrdRecs = prodSet.rdd.map(x=>(x(0).toString, x(1).toString)).groupByKey().mapValues(_.toSet.mkString(",")).toDF("user_id","product_records")
    prodSet.show()
  }
}
