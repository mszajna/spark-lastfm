package lastfm

object ListOps {

  implicit class GroupListWhile[T](list: List[T]) {

    /**
     * Group elements in the list while each two neighbouring elements meet the predicate.
     *
     * Grouping is lazy producing a stream. The overall element's order is preserved.
     *
     * @param p predicate
     * @return a stream of groups of elements
     */
    def groupWhile(p: (T, T) => Boolean): Stream[List[T]] = {

      def groupRecursive(reversedAccumulator: List[T], remaining: List[T]): Stream[List[T]] = remaining match {
        case Nil => if (reversedAccumulator.nonEmpty) Stream(reversedAccumulator.reverse) else Stream.empty
        case x :: xs =>
          if (reversedAccumulator.isEmpty || p(x, reversedAccumulator.head))
            groupRecursive(x :: reversedAccumulator, xs)
          else
            reversedAccumulator.reverse #:: groupRecursive(List(x), xs)
      }

      groupRecursive(List(), list)
    }
  }
}
