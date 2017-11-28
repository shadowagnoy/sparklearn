package cn.jsledd.cn.jsledd.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
  * Type class for de/serializing a Product to/from a Spark SQL [[Row]]
  *
  * @author upio
  */
trait RowFormat[T] extends Serializable {
  /**
    * Schema of the Row
    */
  val schema: StructType

  /**
    * Serialize a Product to a Row
    *
    * @param obj instance
    * @return Spark SQL Row
    */
  def write(obj: T): Row

  /**
    * Deserialize a Spark SQL Row to a Product instance
    *
    * @param row Spark SQL Row representation of T
    * @return deserialized instance
    */
  def read(row: Row): T
}
