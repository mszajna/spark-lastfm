package lastfm

import lastfm.Transformations._
import org.scalatest._

class TransformationsTest extends FlatSpec with Matchers with LocalContext {

  "userDistinctTrackPlayCount" should "contain entry for each user" in {
    val input = context.parallelize(List(
      TrackPlayEvent("12345", 1, Track("track", "artist")),
      TrackPlayEvent("54321", 2, Track("track2", "artist")),
      TrackPlayEvent("12345", 4, Track("track3", "artist"))
    ))
    val result = input.userDistinctTrackPlayCount.collectAsMap()
    result.size should be (2)
  }

  "userDistinctTrackPlayCount" should "count number of plays" in {
    val input = context.parallelize(List(
      TrackPlayEvent("12345", 1, Track("track", "artist")),
      TrackPlayEvent("12345", 2, Track("track2", "artist")),
      TrackPlayEvent("12345", 4, Track("track3", "artist"))
    ))
    val result = input.userDistinctTrackPlayCount.collectAsMap()
    result("12345") should be (3L)
  }

  "userDistinctTrackPlayCount" should "count discard recuring tracks" in {
    val input = context.parallelize(List(
      TrackPlayEvent("12345", 1, Track("track", "artist")),
      TrackPlayEvent("12345", 2, Track("track", "artist")),
      TrackPlayEvent("12345", 4, Track("track", "artist"))
    ))
    val result = input.userDistinctTrackPlayCount.collectAsMap()
    result("12345") should be (1L)
  }

  "tracksPlayCount" should "contain entry for each track" in {
    val input = context.parallelize(List(
      TrackPlayEvent("12345", 1, Track("track", "artist")),
      TrackPlayEvent("12345", 2, Track("track2", "artist")),
      TrackPlayEvent("12345", 4, Track("track3", "artist"))
    ))
    val result = input.tracksPlayCount.collectAsMap()
    result.size should be (3)
  }

  "tracksPlayCount" should "count track plays" in {
    val track = Track("track", "artist")
    val input = context.parallelize(List(
      TrackPlayEvent("12345", 1, track),
      TrackPlayEvent("54321", 2, track),
      TrackPlayEvent("12345", 4, track)
    ))
    val result = input.tracksPlayCount.collectAsMap()
    result(track) should be (3L)
  }

  "sessions" should "put events in the same session if they're within specified timestamp" in {
    val userId = "12345"
    val input = context.parallelize(List(
      TrackPlayEvent(userId, 1, Track("track", "artist")),
      TrackPlayEvent(userId, 2, Track("track2", "artist")),
      TrackPlayEvent(userId, 4, Track("track3", "artist"))
    ))

    val result = input.sessions(1).collect()
    result.length should be(2)
    result(0).length should be(2)
    result(1).length should be(1)
  }

  "sessions" should "have tracks sorted in play order" in {
    val userId = "12345"
    val input = context.parallelize(List(
      TrackPlayEvent(userId, 3, Track("track", "artist")),
      TrackPlayEvent(userId, 2, Track("track2", "artist")),
      TrackPlayEvent(userId, 4, Track("track3", "artist"))
    ))
    val result = input.sessions(1).collect()
    result.forall(session => session.zip(session.tail).forall { case (a, b) => a.timestamp < b.timestamp } )
  }

  "sessions" should "never put different users' events in same session" in {
    val input = context.parallelize(List(
      TrackPlayEvent("12345", 3, Track("track", "artist")),
      TrackPlayEvent("12345", 2, Track("track2", "artist")),
      TrackPlayEvent("54321", 4, Track("track3", "artist"))
    ))
    val result = input.sessions(1).collect()
    result.length should be(2)
  }


}
