/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2011, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala.collection

import scala.collection.generic.CanBuildFrom
/*@PAR*/
import scala.collection.generic.CanCombineFrom
/*PAR@*/
/*@NOPAR
import scala.collection.parallel.generic.CanCombineFrom
PARNO@*/
import scala.collection.parallel.mutable.ParArray
import scala.collection.mutable.UnrolledBuffer
import annotation.unchecked.uncheckedVariance
import language.implicitConversions

/** Package object for parallel collections.
 */
package object parallel {
  /* constants */
  val MIN_FOR_COPY = 512
  val CHECK_RATE = 512
  val SQRT2 = math.sqrt(2)
  val availableProcessors = java.lang.Runtime.getRuntime.availableProcessors

  /* functions */

  /** Computes threshold from the size of the collection and the parallelism level.
   */
  def thresholdFromSize(sz: Int, parallelismLevel: Int) = {
    val p = parallelismLevel
    if (p > 1) 1 + sz / (8 * p)
    else sz
  }

  private[parallel] def unsupported = throw new UnsupportedOperationException

  private[parallel] def unsupportedop(msg: String) = throw new UnsupportedOperationException(msg)

  private[parallel] def outofbounds(idx: Int) = throw new IndexOutOfBoundsException(idx.toString)

  private[parallel] def getTaskSupport: TaskSupport =
    if (util.Properties.isJavaAtLeast("1.6")) {
      val vendor = util.Properties.javaVmVendor
      if ((vendor contains "Oracle") || (vendor contains "Sun") || (vendor contains "Apple")) new ForkJoinTaskSupport
      else new ThreadPoolTaskSupport
    } else new ThreadPoolTaskSupport

  val defaultTaskSupport: TaskSupport = getTaskSupport
  
  def setTaskSupport[Coll](c: Coll, t: TaskSupport): Coll = {
    c match {
      case pc: ParIterableLike[_, _, _] => pc.tasksupport = t
      case _ => // do nothing
    }
    c
  }
  
  /* implicit conversions */

  implicit def factory2ops[From, Elem, To](bf: CanBuildFrom[From, Elem, To]) = new FactoryOps[From, Elem, To] {
    def isParallel = bf.isInstanceOf[Parallel]
    def asParallel = bf.asInstanceOf[CanCombineFrom[From, Elem, To]]
    def ifParallel[R](isbody: CanCombineFrom[From, Elem, To] => R) = new Otherwise[R] {
      def otherwise(notbody: => R) = if (isParallel) isbody(asParallel) else notbody
    }
  }
  implicit def traversable2ops[T](t: collection.GenTraversableOnce[T]) = new TraversableOps[T] {
    def isParallel = t.isInstanceOf[Parallel]
    def isParIterable = t.isInstanceOf[ParIterable[_]]
    def asParIterable = t.asInstanceOf[ParIterable[T]]
    def isParSeq = t.isInstanceOf[ParSeq[_]]
    def asParSeq = t.asInstanceOf[ParSeq[T]]
    def ifParSeq[R](isbody: ParSeq[T] => R) = new Otherwise[R] {
      def otherwise(notbody: => R) = if (isParallel) isbody(asParSeq) else notbody
    }
    def toParArray = if (t.isInstanceOf[ParArray[_]]) t.asInstanceOf[ParArray[T]] else {
      val it = t.toIterator
      val cb = mutable.ParArrayCombiner[T]()
      while (it.hasNext) cb += it.next
      cb.result
    }
  }
  implicit def throwable2ops(self: Throwable) = new ThrowableOps {
    def alongWith(that: Throwable) = (self, that) match {
      case (self: CompositeThrowable, that: CompositeThrowable) => new CompositeThrowable(self.throwables ++ that.throwables)
      case (self: CompositeThrowable, _) => new CompositeThrowable(self.throwables + that)
      case (_, that: CompositeThrowable) => new CompositeThrowable(that.throwables + self)
      case _ => new CompositeThrowable(Set(self, that))
    }
  }

  /*@NOPAR
  import collection.{immutable => ci, mutable => cm}
  import parallel.{immutable => pi, mutable => pm}

  /* collection implicits: Iterable, GenTraversable, GenIterable, GenMap */
  //implicit def iterable2ParIterable[A, Sequential <: collection.Iterable[A] with IterableLike[A, Sequential]](col: IterableLike[A, Sequential]) =
  implicit def iterable2ParIterable[A](col: Iterable[A]) =
    new Parallelizable[A, ParIterable[A]] {
      def seq = col
      protected[this] def parCombiner = ParIterable.newCombiner[A]
    }

  implicit def genTraversable2ParIterable[A](col: GenTraversable[A]) =
    new Parallelizable[A, ParIterable[A]] {
      def seq = col.seq
      protected[this] def parCombiner = ParIterable.newCombiner[A]
    }

  implicit def genIterable2ParIterable[A, T <: GenIterable[A]](col: T) =
    new Parallelizable[A, ParIterable[A]] {
      def seq = col.seq
      protected[this] def parCombiner = ParIterable.newCombiner[A]
    }

  implicit def genMap2ParMap[A, B](col: GenMap[A, B]) =
    new Parallelizable[(A, B), ParMap[A, B]] {
      def seq = col.seq
      protected[this] def parCombiner = ParMap.newCombiner[A, B]
    }
/*
  implicit def genSet2ParSet[A, B](col: GenSet[A]) =
    new Parallelizable[A, ParSet[A]] {
      def seq = col.seq
      protected[this] def parCombiner = ParSet.newCombiner[A]
    }
*/
  /* immutable implicits: Iterable, Map, Seq, Set, HashMap, HashSet, Vector */
  implicit def ciIterable2ParIterable[A, Sequential <: ci.Iterable[A] with IterableLike[A, Sequential]](col: IterableLike[A, Sequential]) =
    new Parallelizable[A, pi.ParIterable[A]] {
      def seq = col
      protected[this] def parCombiner = pi.ParIterable.newCombiner[A]
    }

  implicit def ciMap2ParMap[A, B, Sequential <: ci.Map[A, B] with ci.MapLike[A, B, Sequential]](col: ci.MapLike[A, B, Sequential]) =
    new Parallelizable[(A, B), pi.ParMap[A, B]] {
      def seq = col
      protected[this] def parCombiner = pi.ParMap.newCombiner[A, B]
    }

  implicit def ciSeq2ParSeq[A, Sequential <: ci.Seq[A] with SeqLike[A, Sequential]](col: ci.Seq[A]) =
    new Parallelizable[A, pi.ParSeq[A]] {
      def seq = col
      protected[this] def parCombiner = pi.ParSeq.newCombiner[A]
    }

  implicit def ciSet2ParSet[A, Sequential <: ci.Set[A] with SetLike[A, Sequential]](col: ci.Set[A]) =
    new Parallelizable[A, pi.ParSet[A]] {
      def seq = col
      protected[this] def parCombiner = pi.ParSet.newCombiner[A]
    }

  implicit def ciHashMap2ParHashMap[A, B](col: ci.HashMap[A, B]) =
    new CustomParallelizable[(A, B), pi.ParHashMap[A, B]] {
      def seq = col
      override def par = pi.ParHashMap.fromTrie(col)
      protected[this] override def parCombiner = pi.ParHashMap.newCombiner[A, B]
    }

  implicit def ciHashSet2ParHashSet[A](col: ci.HashSet[A]) =
    new CustomParallelizable[A, pi.ParHashSet[A]] {
      def seq = col
      override def par = pi.ParHashSet.fromTrie(col)
      protected[this] override def parCombiner = pi.ParHashSet.newCombiner[A]
    }

  implicit def ciVector2ParVector[A](col: ci.Vector[A]) =
    new Parallelizable[A, pi.ParVector[A]] {
      def seq = col
      protected[this] def parCombiner = pi.ParVector.newCombiner[A]
    }

  /* mutable implicits: Iterable, Map, Seq, Set, HashMap, HashSet */
  implicit def cmIterable2ParIterable[A, This <: cm.Iterable[A] with IterableLike[A, This]](col: IterableLike[A, This]) =
    new Parallelizable[A, pm.ParIterable[A]] {
      def seq = col
      protected[this] def parCombiner = pm.ParIterable.newCombiner[A]
    }

  implicit def cmMap2ParMap[A, B, This <: cm.Map[A, B] with cm.MapLike[A, B, This]](col: cm.MapLike[A, B, This]) =
    new Parallelizable[(A, B), pm.ParMap[A, B]] {
      def seq = col
      protected[this] def parCombiner = pm.ParMap.newCombiner[A, B]
    }

  implicit def cmSeq2ParSeq[A, This <: cm.Seq[A] with cm.SeqLike[A, This]](col: cm.SeqLike[A, This]) =
    new Parallelizable[A, pm.ParSeq[A]] {
      def seq = col
      protected[this] def parCombiner = pm.ParSeq.newCombiner[A]
    }

  implicit def cmSet2ParSet[A, This <: cm.Set[A] with cm.SetLike[A, This]](col: cm.SetLike[A, This]) =
    new Parallelizable[A, pm.ParSet[A]] {
      def seq = col
      protected[this] def parCombiner = pm.ParSet.newCombiner[A]
    }

  implicit def cmHashMap2ParHashMap[A, B](col: cm.HashMap[A, B]) =
    new CustomParallelizable[(A, B), pm.ParHashMap[A, B]] {
      def seq = col
      override def par = new pm.ParHashMap[A, B](col.hashTableContents)
      protected[this] override def parCombiner = pm.ParHashMap.newCombiner[A, B]
    }

  implicit def cmHashset2ParHashSet[A](col: cm.HashSet[A]) =
    new CustomParallelizable[A, pm.ParHashSet[A]] {
      def seq = col
      override def par = new pm.ParHashSet(col.hashTableContents)
      protected[this] override def parCombiner = pm.ParHashSet.newCombiner[A]
    }

  implicit def cmWrappedArray2ParArray[A](col: cm.WrappedArray[A]) =
    new CustomParallelizable[A, pm.ParArray[A]] {
      def seq = col
      override def par = pm.ParArray.handoff(col.array)
      protected[this] override def parCombiner = pm.ParArray.newCombiner[A]
    }
  PARNO@*/
}


package parallel {
  trait FactoryOps[From, Elem, To] {
    trait Otherwise[R] {
      def otherwise(notbody: => R): R
    }

    def isParallel: Boolean
    def asParallel: CanCombineFrom[From, Elem, To]
    def ifParallel[R](isbody: CanCombineFrom[From, Elem, To] => R): Otherwise[R]
  }

  trait TraversableOps[T] {
    trait Otherwise[R] {
      def otherwise(notbody: => R): R
    }

    def isParallel: Boolean
    def isParIterable: Boolean
    def asParIterable: ParIterable[T]
    def isParSeq: Boolean
    def asParSeq: ParSeq[T]
    def ifParSeq[R](isbody: ParSeq[T] => R): Otherwise[R]
    def toParArray: ParArray[T]
  }

  trait ThrowableOps {
    def alongWith(that: Throwable): Throwable
  }

  /* classes */

  trait CombinerFactory[U, Repr] {
    /** Provides a combiner used to construct a collection. */
    def apply(): Combiner[U, Repr]
    /** The call to the `apply` method can create a new combiner each time.
     *  If it does, this method returns `false`.
     *  The same combiner factory may be used each time (typically, this is
     *  the case for concurrent collections, which are thread safe).
     *  If so, the method returns `true`.
     */
    def doesShareCombiners: Boolean
  }

  /** Composite throwable - thrown when multiple exceptions are thrown at the same time. */
  final case class CompositeThrowable(
    val throwables: Set[Throwable]
  ) extends Exception(
    "Multiple exceptions thrown during a parallel computation: " +
      throwables.map(t => t + "\n" + t.getStackTrace.take(10).++("...").mkString("\n")).mkString("\n\n")
  )


  /** A helper iterator for iterating very small array buffers.
   *  Automatically forwards the signal delegate when splitting.
   */
  private[parallel] class BufferSplitter[T]
  (private val buffer: collection.mutable.ArrayBuffer[T], private var index: Int, private val until: Int, _sigdel: collection.generic.Signalling)
  extends IterableSplitter[T] {
    signalDelegate = _sigdel
    def hasNext = index < until
    def next = {
      val r = buffer(index)
      index += 1
      r
    }
    def remaining = until - index
    def dup = new BufferSplitter(buffer, index, until, signalDelegate)
    def split: Seq[IterableSplitter[T]] = if (remaining > 1) {
      val divsz = (until - index) / 2
      Seq(
        new BufferSplitter(buffer, index, index + divsz, signalDelegate),
        new BufferSplitter(buffer, index + divsz, until, signalDelegate)
      )
    } else Seq(this)
    private[parallel] override def debugInformation = {
      buildString {
        append =>
        append("---------------")
        append("Buffer iterator")
        append("buffer: " + buffer)
        append("index: " + index)
        append("until: " + until)
        append("---------------")
      }
    }
  }

  /** A helper combiner which contains an array of buckets. Buckets themselves
   *  are unrolled linked lists. Some parallel collections are constructed by
   *  sorting their result set according to some criteria.
   *
   *  A reference `buckets` to buckets is maintained. Total size of all buckets
   *  is kept in `sz` and maintained whenever 2 bucket combiners are combined.
   *
   *  Clients decide how to maintain these by implementing `+=` and `result`.
   *  Populating and using the buckets is up to the client. While populating them,
   *  the client should update `sz` accordingly. Note that a bucket is by default
   *  set to `null` to save space - the client should initialize it.
   *  Note that in general the type of the elements contained in the buckets `Buck`
   *  doesn't have to correspond to combiner element type `Elem`.
   *
   *  This class simply gives an efficient `combine` for free - it chains
   *  the buckets together. Since the `combine` contract states that the receiver (`this`)
   *  becomes invalidated, `combine` reuses the receiver and returns it.
   *
   *  Methods `beforeCombine` and `afterCombine` are called before and after
   *  combining the buckets, respectively, given that the argument to `combine`
   *  is not `this` (as required by the `combine` contract).
   *  They can be overriden in subclasses to provide custom behaviour by modifying
   *  the receiver (which will be the return value).
   */
  private[parallel] abstract class BucketCombiner[-Elem, +To, Buck, +CombinerType <: BucketCombiner[Elem, To, Buck, CombinerType]]
  (private val bucketnumber: Int)
  extends Combiner[Elem, To] {
  //self: EnvironmentPassingCombiner[Elem, To] =>
    protected var buckets: Array[UnrolledBuffer[Buck]] @uncheckedVariance = new Array[UnrolledBuffer[Buck]](bucketnumber)
    protected var sz: Int = 0

    def size = sz

    def clear() = {
      buckets = new Array[UnrolledBuffer[Buck]](bucketnumber)
      sz = 0
    }

    def beforeCombine[N <: Elem, NewTo >: To](other: Combiner[N, NewTo]) {}

    def afterCombine[N <: Elem, NewTo >: To](other: Combiner[N, NewTo]) {}

    def combine[N <: Elem, NewTo >: To](other: Combiner[N, NewTo]): Combiner[N, NewTo] = {
      if (this eq other) this
      else other match {
        case _: BucketCombiner[_, _, _, _] =>
          beforeCombine(other)
          val that = other.asInstanceOf[BucketCombiner[Elem, To, Buck, CombinerType]]

          var i = 0
          while (i < bucketnumber) {
            if (buckets(i) eq null)
              buckets(i) = that.buckets(i)
            else if (that.buckets(i) ne null)
              buckets(i) concat that.buckets(i)

            i += 1
          }
          sz = sz + that.size
          afterCombine(other)
          this
        case _ =>
          sys.error("Unexpected combiner type.")
      }
    }
  }
}
