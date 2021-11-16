package org.apache.spark.sql.udaf

import org.apache.spark.sql.{RowFactory, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object CollectBloomFilterDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("MyUDAF")
      .setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(spark)

    val schema = StructType(List(
      StructField("row_id", DataTypes.IntegerType, true),
      StructField("brand", DataTypes.StringType, true)
    ))

    val rdd = sc.parallelize(List(
      RowFactory.create(Int.box(1), "宝洁"),
      RowFactory.create(Int.box(2), "宝洁"),
      RowFactory.create(Int.box(3), "宝洁"),
      RowFactory.create(Int.box(4), "宝洁"),
      RowFactory.create(Int.box(5), "戴森"),
      RowFactory.create(Int.box(6), "戴森"),
      RowFactory.create(Int.box(7), "联合利华"),
      RowFactory.create(Int.box(8), "联合利华")
    ))

    val df = sqlContext.createDataFrame(rdd, schema)
    df.printSchema()
    df.show()

    // udaf:
    spark.udf.registerJavaUDAF("collect_bf", "org.apache.spark.sql.udaf.CollectBloomFilter")
    df.createOrReplaceTempView("tmp")
    spark.sql("select collect_bf(row_id) from tmp group by brand").show()

    spark.stop()
  }

}