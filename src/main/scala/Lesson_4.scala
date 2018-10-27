object Lesson_4 {
  // 生成 case when 文本
  def caseWhenText() : Unit = {
    val a : Array[Int] = Array(1, 2, 3, 4, 5, 6)
    val b : String = a.map(x => "sum(case order_dow when '" + x.toString + "' then 1 else 0 end) as dow_" + x.toString).mkString(",\n")
    println(b)
  }
  // 实现 lcs 算法
  def lcs(a:String, b:String) : Unit = {
    val aLength : Int = a.length
    val bLength : Int = b.length
    val csArray : Array[Array[Int]] = Array.fill(aLength + 1, bLength + 1)(0)
    for (i <- 1 to aLength) {
      for (j <- 1 to bLength) {
        if (a(i-1) == b(j-1)) {
          csArray(i)(j) = csArray(i-1)(j-1) + 1
        } else {
          csArray(i)(j) = Math.max(csArray(i)(j-1), csArray(i-1)(j))
        }
      }
    }
    print("Longest Common Sub-sequence: " + csArray(aLength)(bLength).toString)
  }
  def main(args: Array[String]): Unit = {
//    caseWhenText()
//    lcs("1235", "12345")
  }
}
