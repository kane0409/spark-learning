package dataset

import org.apache.spark.sql.{Dataset, SparkSession}

//
// 创建原始类型和元组类型的数据集并显示简单的操作。
//
object Basic {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("Basic").master("local[*]").getOrCreate()

    import spark.implicits._

    val s = Seq(10, 11, 12, 13, 14, 15)
    val ds: Dataset[Int] = s.toDS()

    ds.columns.foreach(println(_)) //value
    ds.dtypes.foreach(println(_)) //(value,IntegerType)
    ds.printSchema() //|-- value: integer (nullable = false)

    val s2: Seq[Int] = Seq.range(1, 100)
    println(s2.size) //99

    val s3 = Seq((1, "one", "un"), (2, "two", "deux"), (3, "three", "trois"))
    val dsTuple: Dataset[(Int, String, String)] = s3.toDS()
    dsTuple.dtypes.foreach(print(_)) //(_1,IntegerType)(_2,StringType)(_3,StringType)

    //    |   _2|   _3|
    //    +-----+-----+
    //    |three|trois|
    //    +-----+-----+
    dsTuple.where($"_1" > 2).select($"_2", $"_3").show()


  }


}
