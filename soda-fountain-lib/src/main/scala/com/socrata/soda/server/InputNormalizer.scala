package com.socrata.soda.server

import com.ibm.icu.text.Normalizer2
import com.rojoma.json.v3.io._

object InputNormalizer {
  val normalizer = Normalizer2.getNFCInstance

  def normalize(s: String): String = normalizer.normalize(s)

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
