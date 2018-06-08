package sql

import org.apache.spark.sql.SparkSession

object DataSource {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("data source")
      .getOrCreate()

    runBasicDataSource(spark)

    spark.stop()
  }

  def runBasicDataSource(spark: SparkSession): Unit = {
    val basePath = "/user/root/duke/data/parquet/"

    val userDF = spark.read.load(basePath + "users.parquet")
    userDF.select("name", "favorite_color").write.save(basePath + "nameAndFavColors.parquet")
    spark.sql("select * from parquet.`" + basePath + "nameAndFavColors.parquet`").show()

    val peopleDF = spark.read.json("/user/root/duke/data/people.json")
    peopleDF.select("name", "age").write.format("parquet").save(basePath + "namesAndAges.parquet")
    spark.sql("select * from parquet.`" + basePath + "namesAndAges.parquet`").show()

    val sqlDF = spark.sql("select * from parquet.`" + basePath + "users.parquet`").show()
    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    userDF.write.partitionBy("favorite_color").format("parquet").save(basePath + "namesPartByColor.parquet")

    userDF.write.partitionBy("favorite_color").bucketBy(42, "name").saveAsTable("users_partitioned_bucketed")

    spark.sql("select * from parquet.`" + basePath + "users_partitioned_bucketed.parquet`").show()

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")
  }

}
