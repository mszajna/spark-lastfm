package lastfm

import java.io.PrintWriter

trait OutputSupport {
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B = try { f(param) } finally { param.close() }
  def writeTo[T](filename: String)(f: PrintWriter => T): T = using(new PrintWriter(filename))(f)
}
