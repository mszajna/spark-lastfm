package lastfm

import org.apache.spark.rdd.RDD

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
    def sessions(maxTimestampDiff: Long): RDD[Session] = {

      def withinTimeFrame(a: TrackPlayEvent, b: TrackPlayEvent) = Math.abs(a.timestamp - b.timestamp) <= maxTimestampDiff

      //Performs folding step taking sessions found so far and the next track play event
      //either adding the event to the last session or creating a new session
      def collectSessions(sessions: List[Session], currentEvent: TrackPlayEvent): List[Session] = sessions match {
        case (previousEvent :: currentSessionTail) :: previousSessions if withinTimeFrame(currentEvent, previousEvent) =>
          (currentEvent :: previousEvent :: currentSessionTail) :: previousSessions
        case previousSessions => List(currentEvent) :: previousSessions
      }

      //Transforms a list of track play events of a single user to a list of sessions
      //sorting events by timestamp and matching neighbouring events if they are within maxTimestampDiff
      def userEventsToSessions(events: List[TrackPlayEvent]): List[Session] =
        events.sortBy(-_.timestamp).foldLeft(List[Session]())(collectSessions)

      trackPlayEvents
        .groupBy(_.userId)
        .flatMap { case (_, userEvents) => userEventsToSessions(userEvents.toList) }
    }
  }
}
