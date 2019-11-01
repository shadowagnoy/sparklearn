package cn.jsledd.spark.sql

import java.util
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType

/**
  * @author ：jsledd
  * @date ：Created in 2019/4/5 0001 下午 14:12
  * @description：${description}
  * @modified By：
  * @version: $version$
  */

case class CustomDataSourceV2Reader(options: Map[String, String]) extends DataSourceReader {
  /**
    * 读取的列相关信息
    * @return
    */
  override def readSchema(): StructType = ???

  /**
    * 每个分区拆分及读取逻辑
    * @return
    */
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = ???
}
