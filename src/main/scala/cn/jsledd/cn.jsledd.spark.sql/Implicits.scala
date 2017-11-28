package cn.jsledd.cn.jsledd.spark.sql

import org.apache.spark.sql.{ Row}

/**
  * Implicit classes to add convenience methods to Row and Any
  *
  * @author upio
  */
object Implicits {
  implicit class RowWrapper(val row: Row) extends AnyVal {
    /**
      * Convert a Row to a type using an implicit [[RowFormat]]
      * @tparam T type of product
      * @return instance of T
      */
    def convertTo[T: RowFormat]: T = implicitly[RowFormat[T]].read(row)
  }

  implicit class TWrapper[T](val t: T) extends AnyVal {
    /**
      * Convert a Product to a Row
      *
      * @param format implicit RowFormat to convert T to Row
      * @return Spark SQL Row
      */
    def toRow(implicit format: RowFormat[T]): Row = format.write(t)

    def toColumn(implicit format: ColumnFormat[T]): Any = format.write(t)
  }

  implicit class AnyWrapper(val a: Any) extends AnyVal {
    def fromColumn[T](implicit format: ColumnFormat[T]) = format.read(a)
  }
}
