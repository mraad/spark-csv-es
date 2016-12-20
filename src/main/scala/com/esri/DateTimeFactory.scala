package com.esri

import org.joda.time.format.DateTimeFormat

/**
  * @deprecated
  */
object DateTimeFactory extends Serializable {
  def forPattern(pattern: String) = {
    DateTimeFormat.forPattern(pattern).withZoneUTC()
  }
}
