package org.apache.spark.sql.udt

import org.apache.spark.sql.types.SQLUserDefinedType

@SQLUserDefinedType(udt = classOf[PersonUDT])
@SerialVersionUID(100001L)
case class Person(firstName: String, lastName: String, age: Integer) extends Serializable {
  def getFirstName: String = firstName
  def getLastName: String = lastName
  def getAge: Integer = age
}
