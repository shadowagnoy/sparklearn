package cn.jsledd

import shapeless.ops.hlist.Prepend

object CartesianProductExample extends App {

  import shapeless._
  import poly._
  import test._

  /**
    * A type class that helps us partially apply a polymorphic binary function
    * to some value and map the resulting function (which of course isn't
    * literally a Poly1) over an HList.
    */
  trait ApplyMapper[HF, A, X <: HList, Out <: HList] {
    def apply(a: A, x: X): Out
  }

  object ApplyMapper {
    implicit def hnil[HF, A] = new ApplyMapper[HF, A, HNil, HNil] {
      def apply(a: A, x: HNil) = HNil
    }

    implicit def hlist[HF, A, XH, XT <: HList, OutH, OutT <: HList](implicit
                                                                    applied: Case2.Aux[HF, A, XH, OutH],
                                                                    mapper: ApplyMapper[HF, A, XT, OutT]
                                                                   ) = new ApplyMapper[HF, A, XH :: XT, OutH :: OutT] {
      def apply(a: A, x: XH :: XT) = applied(a, x.head) :: mapper(a, x.tail)
    }
  }

  /**
    * A type class that lets us "lift" a polymorphic binary function so that it
    * operates on HLists, in the manner of Haskell's Control.Applicative.liftA2.
    */
  trait LiftA2[HF, X <: HList, Y <: HList, Out <: HList] {
    def apply(x: X, y: Y): Out
  }

  object LiftA2 {
    implicit def hnil[HF, Y <: HList] = new LiftA2[HF, HNil, Y, HNil] {
      def apply(x: HNil, y: Y) = HNil
    }

    implicit def hlist[
    HF, XH, XT <: HList, Y <: HList,
    Out1 <: HList, Out2 <: HList
    ](implicit
      mapper: ApplyMapper[HF, XH, Y, Out1],
      lift: LiftA2[HF, XT, Y, Out2],
      prepend: Prepend[Out1, Out2]
     ) = new LiftA2[HF, XH :: XT, Y, prepend.Out] {
      def apply(x: XH :: XT, y: Y) = prepend(mapper(x.head, y), lift(x.tail, y))
    }
  }

  /**
    * A method that pulls together evidence that some higher rank function can be
    * lifted to work on two HLists.
    */
  def liftA2[HF, X <: HList, Y <: HList, Out <: HList](hf: HF)(x: X, y: Y)(implicit
                                                                           lift: LiftA2[HF, X, Y, Out]
  ) = lift(x, y)

  /**
    * A polymorphic binary function that pairs its arguments.
    */
  object tuple extends Poly {
    implicit def whatever[A, B] = use((a: A, b: B) => (a, b))
  }

  // Two example lists.
  val xs = 1 :: 'b :: 'c' :: HNil
  val ys = 4.0 :: "e" :: HNil

  // The Cartesian product of these lists.
  val result = liftA2(tuple)(xs, ys)

  // The expected type of the Cartesian product.
  type Result =
    (Int, Double) :: (Int, String) ::
      (Symbol, Double) :: (Symbol, String) ::
      (Char, Double) :: (Char, String) :: HNil

  // The expected value.
  val expected =
    (1, 4.0) :: (1, "e") ::
      ('b, 4.0) :: ('b, "e") ::
      ('c', 4.0) :: ('c', "e") :: HNil

  typed[Result](result)

  println(result == expected)
}