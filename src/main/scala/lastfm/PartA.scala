package lastfm

import Transformations._

object PartA extends LocalContext with ParsingSupport with OutputSupport {

  def main(args: Array[String]): Unit = {
    val trackPlayEvents = context.textFile(args(0)).flatMap(parseTrackPlayEvent)
    val userDistinctTrackPlayCount = trackPlayEvents.userDistinctTrackPlayCount.collectAsMap()

    writeTo("outputA.txt") { writer =>
      userDistinctTrackPlayCount.foreach(writer.println)
    }
  }
}
