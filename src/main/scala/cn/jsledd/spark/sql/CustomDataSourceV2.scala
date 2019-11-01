package cn.jsledd.spark.sql

import java.util.Optional
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}

/**
  * @author ：jsledd
  * @date ：Created in 2019/4/5 0001 上午 9:19
  * @description：自定义DataSourceV2 的数据源
  * @modified By：
  * @version: $version$
  */
class CustomDataSourceV2 extends DataSourceV2  with ReadSupport with WriteSupport  {
  /**
    * 创建Reader
    *
    * @param dataSourceOptions 用户自定义的options
    * @return 返回自定义的DataSourceReader
    */
  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = ???
  /**
    * 创建Writer
    *
    * @param jobId   jobId
    * @param schema  schema
    * @param mode    保存模式
    * @param options 用于定义的option
    * @return Optional[自定义的DataSourceWriter]
    */
  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = ???

}
