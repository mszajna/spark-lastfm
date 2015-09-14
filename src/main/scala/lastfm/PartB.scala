package lastfm

import Transformations._

object PartB extends LocalContext with ParsingSupport with OutputSupport {

  def main(args: Array[String]): Unit = {
    val trackPlayEvents = context.textFile(args(0)).flatMap(parseTrackPlayEvent)
    val topTracks = trackPlayEvents.tracksPlayCount.top(100)(Ordering.by(_._2))

    writeTo("outputB.txt") { writer =>
      topTracks.foreach(writer.println)
    }
  }
}
