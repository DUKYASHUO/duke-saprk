package util

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

class Hdfs(context: SparkContext) {

  lazy val hdfs = org.apache.hadoop.fs.FileSystem.get(context.hadoopConfiguration)

  def del(path: String): Boolean = {
    hdfs.delete(new Path(path), true)
  }

}

object Hdfs {

  def apply(context: SparkContext): Hdfs = new Hdfs(context)

}
