package sql

import shapeless._

case class SFoo(i: Int, s: String, b: Foo)

case class Foo(i: Int, s: String, b: Boolean)

case class Bar(b: Boolean, s: String)

object CaseClassMergeDemo extends App {

  import mergeSyntax._

  val foo = Foo(23, "foo", true)
  val bar = Bar(false, "bar")
  val merged = foo merge bar
  println(merged == Foo(23, "bar", false))
}

// Implementation in terms of LabelledGeneric ...
object mergeSyntax {

  implicit class MergeSyntax[T](t: T) {
    def merge[U](u: U)(implicit merge: CaseClassMerge[T, U]): T = merge(t, u)
  }

}

trait CaseClassMerge[T, U] {
  def apply(t: T, u: U): T
}

object CaseClassMerge {

  import ops.record.Merger

  def apply[T, U](implicit merge: CaseClassMerge[T, U]): CaseClassMerge[T, U] = merge

  implicit def mkCCMerge[T, U, RT <: HList, RU <: HList]
  (implicit
   tgen: LabelledGeneric.Aux[T, RT],
   ugen: LabelledGeneric.Aux[U, RU],
   merger: Merger.Aux[RT, RU, RT]
  ): CaseClassMerge[T, U] =
    new CaseClassMerge[T, U] {
      def apply(t: T, u: U): T =
        tgen.from(merger(tgen.to(t), ugen.to(u)))
    }
}

trait Show[A] {
  def show(a: A): String
}

/**
  * Created by Administrator on 2017/10/12 0012.
  */
object Spark {

  import cn.jsledd.cn.jsledd.spark.sql._
  import ColumnFormats._
  import FamilyFormats._
  def main(args: Array[String]): Unit = {
    val format1 = rowFormat[Bar]
    format1.schema.printTreeString()
  }
}
