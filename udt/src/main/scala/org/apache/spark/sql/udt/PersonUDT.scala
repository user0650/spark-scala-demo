package org.apache.spark.sql.udt

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[spark] class PersonUDT extends UserDefinedType[Person]{

  override def sqlType: DataType = StructType(List(
    StructField("firstName", StringType, false),
    StructField("lastName", StringType, false),
    StructField("age", IntegerType, false)
  ))

  override def serialize(obj: Person): Any = {
    val row = new GenericInternalRow(3)
    row.update(0, UTF8String.fromString(obj.getFirstName))
    row.update(1, UTF8String.fromString(obj.getLastName))
    row.update(2, obj.getAge)
    row
  }

  override def deserialize(datum: Any): Person = {
    val row = datum.asInstanceOf[InternalRow]
    require(row.numFields == 3, s"PersonUDT.deserialize 参数不正确！")
    Person(row.getString(0), row.getString(1), row.getInt(2))
  }

  override def userClass: Class[Person] = classOf[Person]
}
