package lastfm

import org.apache.spark.rdd.RDD
import lastfm.ListOps._

object Transformations {

  type Session = List[TrackPlayEvent]

  implicit class TrackPlayEventTransformations(trackPlayEvents: RDD[TrackPlayEvent]) extends Serializable {

    /**
     * Counts how many distinct tracks each user played
     *
     * @return pairs of user id and the number of distinct tracks played
     */
    def userDistinctTrackPlayCount: RDD[(String, Long)] =
      trackPlayEvents
        .map(event => (event.userId, event.track))
        .distinct()
        .mapValues(_ => 1L)
        .reduceByKey(_ + _)

    /**
     * Counts how many times each track has been played.
     *
     * @return pairs of <code>Track</code> and play count
     */
    def tracksPlayCount: RDD[(Track, Long)] =
      trackPlayEvents
        .map(event => (event.track, 1L))
        .reduceByKey(_ + _)

    /**
     * Groups track play events into sessions.
     *
     * A session is defined as a series of track play events where each event happened
     * within <code>maxTimestampDiff<code> milliseconds from the previous one.
     *
     * Track play events within session are sorted by the time they were played ascending.
     *
     * @param maxTimestampDiff maximum time difference between two neighbouring events
     * @return track play events grouped in sessions
     */
    def sessions(maxTimestampDiff: Long): RDD[Session] =
      trackPlayEvents
        .groupBy(_.userId)
        .values
        .map(_.toList.sortBy(_.timestamp))
        .flatMap(_.groupWhile((a, b) => Math.abs(a.timestamp - b.timestamp) <= maxTimestampDiff))
  }
}
