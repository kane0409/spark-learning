package dataset

import org.apache.spark.sql.{Dataset, SparkSession}

//
// Create Datasets of primitive type and tuple type ands show simple operations.
//
object Basic {

  private val spark: SparkSession = SparkSession.builder().appName("Basic").master("local[*]").getOrCreate()

  import spark.implicits._

  val s = Seq(10,11,12,13,14,15)
  val ds: Dataset[Int] = s.toDS()

}
