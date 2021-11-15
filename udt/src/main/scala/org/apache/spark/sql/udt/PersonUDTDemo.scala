package org.apache.spark.sql.udt

import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object PersonUDTDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("MyUDT")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val schema = StructType(List(
      StructField("person_id", DataTypes.IntegerType, true),
      StructField("person", new PersonUDT, true)
    ))

    val rdd = sc.parallelize(List(
      RowFactory.create(Int.box(1), Person("zhang", "san", 25)),
      RowFactory.create(Int.box(2), Person("li", "si", 28))
    ))

    val df = sqlContext.createDataFrame(rdd, schema)
    df.printSchema()
    df.show()

    sc.stop()
  }

}