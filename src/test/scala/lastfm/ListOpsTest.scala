package lastfm

import org.scalatest._
import lastfm.ListOps._

class ListOpsTest extends FlatSpec with Matchers {

  "groupWhile" should "group elements while they match together" in {
    val result = List(true, true, false, true).groupWhile(_ == _)
    result.length should be (3)
    result(0) should be (List(true, true))
    result(1) should be (List(false))
    result(2) should be (List(true))
  }

  "groupWhile" should "preserve order" in {
    val result = List(1, 2, 3, 11, 12, 13).groupWhile((a, b) => b - a == 1)
    result.length should be (2)
    result(0) should be (1, 2, 3)
    result(1) should be (11, 12, 13)
  }
}
