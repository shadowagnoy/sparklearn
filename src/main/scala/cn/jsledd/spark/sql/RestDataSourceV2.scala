package cn.jsledd.spark.sql

import java.util
import java.util.Optional

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * @author ：jsledd
  * @date ：Created in 2019/4/5 0001 下午 14:37
  * @description：基于Rest的Spark SQL DataSource
  * @modified By：
  * @version: $version$
  */
class RestDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport {

  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = new RestDataSourceReader(dataSourceOptions)

  override def createWriter(s: String, structType: StructType, saveMode: SaveMode, dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = ???
}

/**
  * 创建RestDataSourceReader
  *
  * @param dataSourceOptions dataSourceOptions 参数集合
  *
  */
class RestDataSourceReader(dataSourceOptions: DataSourceOptions) extends DataSourceReader {
  // 使用StructType.fromDDL方法将schema字符串转成StructType类型
  val schemaString = dataSourceOptions.get("schema").get()

  var requiredSchema: StructType = StructType.fromDDL(schemaString)

  val supportedFilters: ArrayBuffer[Filter] = ArrayBuffer[Filter]()

  override def readSchema(): StructType = requiredSchema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {

    List[InputPartition[InternalRow]](RestInputPartition(requiredSchema, supportedFilters.toArray, dataSourceOptions)).asJava

  }
}

class RestDataSourceWriter(dataSourceOptions: DataSourceOptions) extends DataSourceWriter {
  /**
    * 创建RestDataWriter工厂类
    *
    * @return RestDataWriterFactory
    */
  override def createWriterFactory(): DataWriterFactory[InternalRow] = ???

  /**
    * commit
    *
    * @param writerCommitMessages 所有分区提交的commit信息
    *                 触发一次
    */
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = ???

  /** *
    * abort
    *
    * @param writerCommitMessages 当write异常时调用
    */
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = ???
}

/**
  * DataWriterFactory工厂类
  */
class RestDataWriterFactory extends DataWriterFactory[Row] {
  /**
    * 创建DataWriter
    *
    * @param partitionId 分区ID
    * @param taskId      task ID
    * @param epochId     一种单调递增的id，用于将查询分成离散的执行周期。对于非流式查询，此ID将始终为0。
    */
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[Row] = new RestDataWriter(partitionId)
}

/**
  * RestDataWriter
  *
  * @param partitionId   分区ID
  */
class RestDataWriter(partitionId: Int) extends DataWriter[Row] {
  /**
    * write
    *
    * @param record 单条记录
    *               每条记录都会触发该方法
    */
  override def write(record: Row): Unit = {

    println(record)
  }

  /**
    * commit
    *
    * @return commit message
    *         每个分区触发一次
    */
  override def commit(): WriterCommitMessage = ???


  /**
    * 回滚：当write发生异常时触发该方法
    */
  override def abort(): Unit = {
    println("abort 方法被出发了")
  }
}

case class RestInputPartition(requiredSchema: StructType, pushed: Array[Filter], dataSourceOptions: DataSourceOptions) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = RestInputPartitionReader(requiredSchema, pushed, dataSourceOptions)
}

case class RestInputPartitionReader(requiredSchema: StructType, pushed: Array[Filter], dataSourceOptions: DataSourceOptions) extends InputPartitionReader[InternalRow] {

  // 使用Iterator模拟数据, 假设有很多数据
  val data: Iterator[Seq[AnyRef]] = getIterator

  def getIterator: Iterator[Seq[AnyRef]] = {
    null
  }

  override def next(): Boolean = data.hasNext

  override def get(): InternalRow = {
    val seq = data.next().map {
      // 浮点类型会自动转为BigDecimal，导致Spark无法转换
      case decimal: BigDecimal =>
        decimal.doubleValue()
      case x => x
    }
    InternalRow(seq: _*)
  }

  override def close(): Unit = {
    println("close source")
  }
}