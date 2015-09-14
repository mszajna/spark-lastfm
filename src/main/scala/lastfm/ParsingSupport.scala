package lastfm

trait ParsingSupport {

  def logParseError(line: String) = Console.err.println(s"Could not parse line: $line")

  def parseTrackPlayEvent(line: String) =
    TrackPlayEventParser.parse(line).fold(Some(_), line => { logParseError(line); None })
}
