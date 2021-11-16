package org.apache.spark.sql.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
 * Demo: 聚合元素列表，拼接为字符串
 */
class CollectBloomFilter extends UserDefinedAggregateFunction{

  /**
   * UDAF输入数据类型定义，my_udaf(a_col)，a_col字段类型
   */
  override def inputSchema: StructType = {
    StructType(List(
      StructField("row_id", DataTypes.IntegerType)
    ))
  }

  /**
   * 缓冲区类型定义
   */
  override def bufferSchema: StructType = {
    StructType(List(
      StructField("hex", DataTypes.StringType)
    ))
  }

  /**
   * udaf输出结果类型定义
   */
  override def dataType: DataType = {
    DataTypes.StringType
  }

  /**
   * 一致性：多次相同输入，输出的结果是否一致
   */
  override def deterministic: Boolean = true

  /**
   * 初始缓冲区
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, "")
  }

  /**
   * 更新缓冲区
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val rowId = input.getInt(0)
    buffer.update(0, rowId.toString)
  }

  /**
   * 合并缓冲区
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getString(0) + buffer2.getString(0))
  }

  /**
   * udaf最终结果
   */
  override def evaluate(buffer: Row): Any = buffer.getString(0)

}
