package lastfm

import org.joda.time.format.ISODateTimeFormat

object TrackPlayEventParser {
  val dateTimeFormat = ISODateTimeFormat.dateTimeNoMillis

  val isoDateTime = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z"
  val string = "[^\\t]+"
  val trackPlayEvent = s"($string)\\t($isoDateTime)\\t($string)?\\t($string)\\t($string)?\\t($string)".r

  /**
   * Parse a line of tab separated input.
   *
   * Columns are
   * (user id, date and time, artist id, artist name, track id, track name)
   *
   * @param line a line of input
   * @return <code>Either</code> of <code>TrackPlayEvent</code> or the input line if failed to parse
   */
  def parse(line: String): Either[TrackPlayEvent, String] = line match {
    case trackPlayEvent(userId, dateTime, artistId, artistName, trackId, trackName) =>
      Left(TrackPlayEvent(userId, dateTimeFormat.parseDateTime(dateTime).getMillis, Track(trackName, artistName)))
    case _ => Right(line)
  }
}
