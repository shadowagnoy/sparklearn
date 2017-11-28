package cn.jsledd.cn.jsledd.spark.sql

import java.time.Instant

import cn.jsledd.Foo
import com.cmcc.family.entity.PeriodicReportNodeAdd
import com.cmcc.family.json.ProcessRawData
import com.cmcc.family.spark.sql.format.rowFormat
import com.cmcc.mid.softdetector.json.{PeriodicReportNode, ProgramReportNode}
import me.limansky.beanpuree.{BeanGeneric, BeanLabelling, LabelledBeanGeneric}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import shapeless.HNil
import shapeless.ops.hlist.Length

/**
  * Created by jingshuai on 2017/11/13 0013.
  */
object TestFormat {
  import com.cmcc.family.spark.sql.format.ColumnFormats._
  import com.cmcc.family.spark.sql.format.FamilyFormats._
  import com.cmcc.family.spark.sql.format._
  import com.cmcc.family.spark.sql.format.Implicits._
  case class A(var a:String,var b:String)
  case class Bar(b: Boolean, s: A)
  case class FlatClass(a: String)
  case class NestedClass(a: String, b: FlatClass)
  case class ClassWithOption(a: Option[String])
  case class NestedClassWithCustomType(a: String, b: FlatClass, c: Seq[String], d: Array[String], e: Map[String, String])
  def main(args: Array[String]): Unit = {
    //val format1 = rowFormat[Foo2]
    //val format = rowFormat[Bar]
    import shapeless._
    val i = productLength[Foo]
    //val x = productLength[Foo2]
    println(i)
    //println(x)
    val add = new PeriodicReportNodeAdd
    add.setCityCode("5555")
    add.setVendorcode("ddddddd")
    var sss = Generic[Foo2]

    //var sss2 = BeanGeneric[Foo2]
    //format.printlnSchema
    //format1.printlnSchema
    //val write1 = format1.write(add)
    //val write = format.write(Bar(true,A("a","b")))
    //println(write1)
  }
}


