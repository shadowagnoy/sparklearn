package cn.jsledd.cn.jsledd.spark

import org.apache.spark.sql.types._

/**
  * @author upio
  */
package object sql {

  /**
    * Utility method for implicitly generating a RowFormat for type T
    * @tparam T type to generate RowFormat for
    * @return RowFormat for type T
    */
  def rowFormat[T: RowFormat]: RowFormat[T] = implicitly[RowFormat[T]]

  def columnFormat[T: ColumnFormat]: ColumnFormat[T] = implicitly[ColumnFormat[T]]

  def productLength[T: ProductLength]: Int = implicitly[ProductLength[T]].value

  def printSchema[T: RowFormat] = {
    val format = rowFormat[T]
    def spaces(depth: Int): String = " " * (depth match {
      case 0 | 1 => 0
      case x => x
    })

    def formatSchema(schema: Any, depth: Int = 1): String = schema match {
      case StructType(fields) => "{\n" + fields.map(formatSchema(_, depth + 1)).mkString("\n") + "\n" + spaces(depth) + "}"
      case StructField(name, datatype, true, _) => spaces(depth + 1) + s"$name: Option[${formatType(datatype, depth)}]"
      case StructField(name, datatype, _, _) => spaces(depth + 1) + s"$name: ${formatType(datatype, depth)}"
    }

    def formatType(dataType: DataType, depth: Int): String = dataType match {
      case StringType => "String"
      case IntegerType => "Int"
      case LongType => "Long"
      case BooleanType => "Boolean"
      case FloatType => "Float"
      case DoubleType => "Double"
      case ArrayType(dt, _) => s"Array[${formatType(dt, depth )}]"
      case MapType(key, value, _) => s"Map[${formatType(key, depth)}, ${formatType(value, depth)}]"

      case s: StructType => formatSchema(s, depth + 1)
    }

    println(formatSchema(format.schema))
  }
}
