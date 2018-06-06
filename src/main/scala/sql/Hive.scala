package sql

import java.io.File
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class Record(id: Int, name: String)

object Hive {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession
    .builder()
    .appName("spark sql test")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  import spark.sql

  def main(args: Array[String]): Unit = {

    sql("use duke")

    // 简单的查询
    val sqlDF = sql("select id, name from user where id < 4 order by id")
    val stringDS = sqlDF.map {
      case Row(id: Long, name: String) =>
        s"id: $id, name: $name"
    }
    stringDS.show()

    // 临时view
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createTempView("records")
    sql("select * from records r join user u on r.id = u.id").show()

    // 创建表
    sql("create table if not exists hive_records (id bigint, name string, created_time string, updated_time string)")
    val df = spark.table("user")
    // 使用insertInto 而不是 saveAsTable
    // https://stackoverflow.com/questions/47844808/what-are-the-differences-between-saveastable-and-insertinto-in-different-savemod?rq=1
    df.write.mode(SaveMode.Overwrite).insertInto("hive_records")
    val sqlRecords = sql("select * from duke.hive_records")
    sqlRecords.show()

    // 创建外部表
    val dataDir = "/user/root/duke/hive_ints"
    spark.range(10).write.parquet(dataDir)
    sql(s"create external table hive_ints(id bigInt) stored as parquet location '$dataDir'")
    sql("select * from hive_ints").show()

    // 创建分区表
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    df.write.partitionBy("id").format("hive").saveAsTable("hive_part_tbl")
    sql("select * from hive_part_tbl").show()

    spark.stop()
  }
}
