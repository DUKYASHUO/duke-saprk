package sql

import util.Hdfs
import org.apache.spark.sql.SparkSession

object DataSource {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("data source")
      .getOrCreate()

    val basePath = "/user/root/duke/data/parquet/"

    val destPath = basePath + "dest/"

    val srcPath = basePath + "src/"

    init(spark, destPath)
    runBasicDataSource(spark, srcPath, destPath)

    spark.stop()
  }

  def init(spark: SparkSession, destPath: String): Unit = {
    Hdfs(spark.sparkContext).del(destPath)
    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")
  }

  def runBasicDataSource(spark: SparkSession, srcPath: String, destPath: String): Unit = {

    val userDF = spark.read.load(srcPath + "users.parquet")
    userDF.select("name", "favorite_color").write.save(destPath + "nameAndFavColors.parquet")
    spark.sql("select * from parquet.`" + destPath + "nameAndFavColors.parquet`").show()

    val peopleDF = spark.read.json("/user/root/duke/data/people.json")
    peopleDF.select("name", "age").write.format("parquet").save(destPath + "namesAndAges.parquet")
    spark.sql("select * from parquet.`" + destPath + "namesAndAges.parquet`").show()

    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    userDF.write.partitionBy("favorite_color").format("parquet").save(destPath + "namesPartByColor.parquet")
    spark.sql("select * from parquet.`" + destPath + "namesPartByColor.parquet`").show()
  }

}
