package core

import org.apache.spark.{SparkConf, SparkContext}

object BasicCore {

  val conf = new SparkConf().setAppName("basic core")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val lines = sc.textFile("/user/root/duke/data/data.txt")

    // 一共有多少行
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    printDiy(totalLength)

    // 每行的字符简单的处理
    val funRdd = lines.map(MyFunction.fun)
    val funReduce = funRdd.reduce((a, b) => a + "<==>" + b)
    printDiy(funReduce)

    // word count
    val pairs = lines.flatMap(_.split(" ")).map(s => (s, 1))
    val counts = pairs.reduceByKey(_+_)
    counts.collect().foreach(println)

    // broadcastVar
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    printDiy(broadcastVar.value)

    //计数器
    val accm = sc.longAccumulator("duke accumulator")
    sc.parallelize(Array(1,2,3,4)).foreach(x => accm.add(x))
    printDiy(accm.value)
  }

  def printDiy(str: Any) = {
    println("-------------------------")
    println(str)
    println("-------------------------")
  }
}

object MyFunction {
  def fun(s: String): String = {
    s + "123123123"
  }
}

