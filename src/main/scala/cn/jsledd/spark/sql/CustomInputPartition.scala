package cn.jsledd.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types._

/**
  * @author ：jsledd
  * @date ：Created in 2019/4/5 0001 下午 14:21
  * @description：定义每个分区具体是如何读取的
  * @modified By：
  * @version: $version$
  */

case class CustomInputPartition(requiredSchema: StructType, pushed: Array[Filter], options: Map[String, String]) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = ???
}

case class CustomInputPartitionReader(requiredSchema: StructType, pushed: Array[Filter], options: Map[String, String]) extends InputPartitionReader[InternalRow] {

  override def next(): Boolean = ???

  override def get(): InternalRow = ???

  override def close(): Unit = ???
}
