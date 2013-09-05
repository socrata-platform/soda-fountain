package com.socrata.soda.server

import com.ibm.icu.text.Normalizer
import com.rojoma.json.io._

object InputNormalizer {
  val normalizationMode: Normalizer.Mode = Normalizer.NFC

  def normalize(s: String): String = Normalizer.normalize(s, normalizationMode)

  def normalizeEvent(event: JsonEvent): JsonEvent = {
    def p(e: JsonEvent) = { e.position = event.position; e }
    event match {
      case StringEvent(s) => p(StringEvent(normalize(s)))
      case FieldEvent(s) => p(FieldEvent(normalize(s)))
      case IdentifierEvent(s) => p(IdentifierEvent(normalize(s)))
      case other => other
    }
  }
}
