import org.apache.spark.sql.SparkSession

object CoreSpark {

  def main(args: Array[String]): Unit = {
    val logFile = args(0)
    val spark = SparkSession.builder().appName("stu spark").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"lines a : $numAs, lines with b $numBs")
    spark.stop()
  }
}
