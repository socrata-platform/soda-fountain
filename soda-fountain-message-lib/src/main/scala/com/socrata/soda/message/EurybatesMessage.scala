package com.socrata.soda.message

import com.socrata.eurybates.Tag

object EurybatesMessage {
  val EurybatesMessageSample1 = "EURYBATES_MESSAGE_SAMPLE_1"

  def tag(message: Message): Tag = message match {
    case _: EurybatesSampleMessage1 => EurybatesMessageSample1
    case _ => ""
  }
}
