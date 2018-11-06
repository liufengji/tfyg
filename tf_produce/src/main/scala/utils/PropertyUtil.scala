package utils

import java.util.Properties

object PropertyUtil {

  val properties = new Properties

  try {
    val stream = ClassLoader.getSystemResourceAsStream("kafka.properties")
    properties.load(stream)
  } catch {
    case e:Exception => e.getStackTrace
  } finally {}

  def getProperties(key:String) = properties.getProperty(key)

}
