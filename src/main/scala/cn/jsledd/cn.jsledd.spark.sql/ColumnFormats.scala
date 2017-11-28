package cn.jsledd.cn.jsledd.spark.sql

import java.sql.{Date, Timestamp}

import scala.reflect._
import cn.jsledd.cn.jsledd.spark.sql.Implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import sql.Show

/**
  * Default [[ColumnFormat]] and [[RowFormat]] generators for commonly used Spark SQL types
  *
  * @author upio
  */
trait ColumnFormats extends Serializable {
  implicit val intShow: Show[Int] = new Show[Int] {
    def show(a: Int) = a.toString
  }
  implicit val stringShow: Show[String] = new Show[String] {
    def show(a: String) = "\"" + a + "\""
  }
  implicit val booleanShow: Show[Boolean] = new Show[Boolean] {
    def show(a: Boolean) = if (a) "true" else "false"
  }
  implicit val booleanColumn: ColumnFormat[Boolean] = DefaultColumnFormat[Boolean](BooleanType)
  implicit val byteColumn: ColumnFormat[Byte] = DefaultColumnFormat[Byte](ByteType)
  implicit val shortColumn: ColumnFormat[Short] = DefaultColumnFormat[Short](ShortType)
  implicit val intColumn: ColumnFormat[Int] = DefaultColumnFormat[Int](IntegerType)
  implicit val longColumn: ColumnFormat[Long] = DefaultColumnFormat[Long](LongType)
  implicit val floatColumn: ColumnFormat[Float] = DefaultColumnFormat[Float](FloatType)
  implicit val doubleColumn: ColumnFormat[Double] = DefaultColumnFormat[Double](DoubleType)

  implicit val stringColumn: ColumnFormat[String] = DefaultColumnFormat[String](StringType)
  implicit val byteSeqColumn: ColumnFormat[Array[Byte]] = DefaultColumnFormat[Array[Byte]](BinaryType)
  implicit val timestampColumn: ColumnFormat[Timestamp] = DefaultColumnFormat[Timestamp](TimestampType)
  implicit val dateColumn: ColumnFormat[Date] = DefaultColumnFormat[Date](DateType)

  implicit def optionFormat[T: ColumnFormat]: ColumnFormat[Option[T]] = new OptionColumnFormat[T]

  implicit def arrayColumn[T : ColumnFormat : ClassTag]: ColumnFormat[Array[T]] = new ColumnFormat[Array[T]] {
    override val dataType: DataType = ArrayType(columnFormat[T].dataType)

    override def write(arr: Array[T]): Any = arr.map(_.toColumn)
    override def read(a: Any): Array[T] = a match {
      case arr: Array[Any] => arr.map(_.fromColumn[T])
      case x => throw ColumnDeserializationException(s"Expected a $dataType, got $a")
    }
  }

  implicit def seqColumn[T: ColumnFormat]: ColumnFormat[Seq[T]] = new ColumnFormat[Seq[T]] {
    override val dataType: DataType = ArrayType(columnFormat[T].dataType)

    override def write(seq: Seq[T]): Any = seq.map(_.toColumn)
    override def read(a: Any): Seq[T] = a match {
      case s: Seq[Any] => s.map(_.fromColumn[T])
      case x => throw ColumnDeserializationException(s"Expected a $dataType, got $a")
    }
  }

  implicit def mapColumn[K: ColumnFormat, V: ColumnFormat]: ColumnFormat[Map[K, V]] = new ColumnFormat[Map[K, V]] {
    override val dataType: DataType = MapType(columnFormat[K].dataType, columnFormat[V].dataType)

    override def write(map: Map[K, V]): Any = map.map {
      case (k, v) => k.toColumn -> v.toColumn
    }
    override def read(a: Any) = a match {
      case map: Map[_, _] => map.map { case (k, v) => k.fromColumn[K] -> v.fromColumn[V] }
      case x => throw ColumnDeserializationException(s"Expected a $dataType, got $a")
    }
  }

  // Implicit generator to support nested products
  implicit def structColumn[T : RowFormat : ClassTag]: ColumnFormat[T] = new ColumnFormat[T] {
    val format = rowFormat[T]

    override val dataType: DataType = format.schema

    override def write(t: T): Any = t.toRow

    override def read(a: Any): T = a match {
      case row: Row => row.convertTo[T]
      case x => throw ColumnDeserializationException(s"expected a $dataType, got $a")
    }
  }

  object CustomColumnFormat {
    /**
      * Function to generate a [[ColumnFormat]] for a custom type by providing a mapping between it and
      * an already supported type.
      *
      * @param to mapping function between TCustom and TSpark
      * @param from mapping function between TSpark and TCustom
      * @tparam TCustom custom type to generate a [[ColumnFormat]] for
      * @tparam TSpark type that is in the [[ColumnFormat]] type class
      * @return a [[ColumnFormat]] for TCustom
      */
    def apply[TCustom : ClassTag, TSpark : ColumnFormat](to: TCustom => TSpark, from: TSpark => TCustom): ColumnFormat[TCustom] = new ColumnFormat[TCustom] {
      override val dataType: DataType = columnFormat[TSpark].dataType

      override def write(custom: TCustom): Any = to(custom).toColumn
      override def read(a: Any): TCustom = from(a.fromColumn[TSpark])
    }
  }
}

object ColumnFormats extends ColumnFormats

class OptionColumnFormat[T: ColumnFormat] extends ColumnFormat[Option[T]] {
  override val dataType: DataType = columnFormat[T].dataType

  override def write(t: Option[T]) = t.map(_.toColumn).orNull
  override def read(a: Any) = Option(a).map(_.fromColumn[T])
}

/**
  * Type class for de/serializing elements to/from Spark SQL row types
  */
abstract class ColumnFormat[T: ClassTag] extends Serializable {
  private val typeTag = classTag[T]

  /**
    * DataType of this column
    */
  val dataType: DataType

  /**
    * Convert T to a type compatible with Spark SQL Rows
    *
    * By default we just return the value since most types are compatible
    *
    * @param t instance to convert
    * @return Spark SQL representation
    */
  def write(t: T): Any = t

  /**
    * Convert a Spark SQL instance to T
    *
    * By default we cast the Any to T as most types are compatible
    *
    * @param a Spark SQL representation
    * @return deserialized instance
    */
  def read(a: Any): T = a match {
    case typeTag(casted) => casted
    case _ => throw ColumnDeserializationException(s"Expected $dataType, got $a")
  }
}

/**
  * Constructor for a default column format which uses the default serialization and deserialization strategy
  *
  * @param dataType type of this column
  */
case class DefaultColumnFormat[T: ClassTag](override val dataType: DataType) extends ColumnFormat[T]

/**
  * Exception thrown when deserialization of a column fails
  *
  * @param msg description of failure
  * @param cause of failure
  */
case class ColumnDeserializationException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)

/**
  * Exception thrown when deserialization of a row fails
  * @param msg description of failure
  * @param cause of failure
  */
case class RowDeserializationException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)
