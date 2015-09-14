package lastfm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

trait LocalContext {

  lazy val context: SparkContext = {
    val processorCount = Runtime.getRuntime.availableProcessors()
    new SparkContext(new SparkConf().setAppName("lastfm").setMaster(s"local[$processorCount]"))
  }
}
