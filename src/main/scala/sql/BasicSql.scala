package sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object BasicSql {
  val spark = SparkSession
    .builder()
    .appName("data frames")
    .getOrCreate()

  import spark.implicits._

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    runBasicDataFrame(spark)
    runBasicDataSet(spark)
    runInferSchema(spark)
    runProgrammaticSchema(spark)
    spark.stop()
  }

  def runBasicDataFrame(spark: SparkSession): Unit = {

    val df = spark.read.json("/user/root/duke/data/people.json")

    df.show()

    df.printSchema()

    df.select("name").show()

    df.select($"name", $"age" + 1).show()

    df.filter($"age" > 21).show()

    df.groupBy("age").count().show()

    df.createTempView("people")

    val sqlDF = spark.sql("select * from people")

    sqlDF.show()

    df.createGlobalTempView("people")

    spark.sql("select * from global_temp.people").show()

    spark.newSession().sql("select * from global_temp.people")

  }

  def runBasicDataSet(spark: SparkSession): Unit = {

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1,2,3).toDS()
    primitiveDS.map(_ + 1).collect().foreach(println)

    val path = "/user/root/duke/data/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

  }

  def runInferSchema(spark: SparkSession): Unit = {
    val path = "/user/root/duke/data/people.txt"

    val peopleDF = spark.sparkContext
      .textFile(path)
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toLong))
      .toDF()

    peopleDF.createOrReplaceTempView("people")

    val teenagerDF = spark.sql("select * from people")

    teenagerDF.map(teenager => "name: " + teenager(0)).show()

    teenagerDF.map(teenager => "name: " + teenager.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    teenagerDF.map(teenager => teenager.getValuesMap[Any](List("name", "age")))
      .collect().foreach(println)

  }

  def runProgrammaticSchema(spark: SparkSession): Unit = {
    val path = "/user/root/duke/data/people.txt"

    val peopleRDD = spark.sparkContext.textFile(path)

    val schemaString = "name age"

    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    val rowRDD = peopleRDD.map(_.split(",")).map(attr => Row(attr(0), attr(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.createOrReplaceTempView("people")

    val results = spark.sql("select name from people")

    results.map(attr => "name: " + attr(0)).show()
  }
}
