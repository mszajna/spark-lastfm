package lastfm

import lastfm.Transformations._
import org.joda.time.format.ISODateTimeFormat

import scala.concurrent.duration._


object PartC extends LocalContext with ParsingSupport with OutputSupport {

  def printTimestamp(timestamp: Long) = ISODateTimeFormat.dateTimeNoMillis.print(timestamp)

  def main(args: Array[String]): Unit = {
    val trackPlayEvents = context.textFile(args(0)).flatMap(parseTrackPlayEvent)
    val longestSessions = trackPlayEvents.sessions(20.minutes.toMillis).top(10)(Ordering.by(_.size))

    writeTo("outputC.txt") { writer =>
      longestSessions
        .map(s => (s.head.userId, printTimestamp(s.head.timestamp), printTimestamp(s.last.timestamp), s.map(_.track)))
        .foreach(writer.println)
    }


  }
}
