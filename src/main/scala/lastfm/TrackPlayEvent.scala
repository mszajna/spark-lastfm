package lastfm

case class Track(trackName: String, artistName: String)

case class TrackPlayEvent(userId: String, timestamp: Long, track: Track)
