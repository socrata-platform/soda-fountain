package com.socrata.soda.server

import com.ibm.icu.text.Normalizer
import com.rojoma.json.v3.io._

object InputNormalizer {
  val normalizationMode: Normalizer.Mode = Normalizer.NFC

  def normalize(s: String): String = Normalizer.normalize(s, normalizationMode)

  def normalizeEvent(event: JsonEvent): JsonEvent = {
    def p(e: JsonEvent) = { e.positionedAt(event.position) }

    event match {
      case StringEvent(s) => p(StringEvent(normalize(s))(event.position))
      case FieldEvent(s) => p(FieldEvent(normalize(s))(event.position))
      case IdentifierEvent(s) => p(IdentifierEvent(normalize(s))(event.position))
      case other => other
    }
  }
}
