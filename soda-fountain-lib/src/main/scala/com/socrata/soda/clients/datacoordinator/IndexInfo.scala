package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.soda.server.id.IndexName

case class IndexInfo(name: IndexName, expressions: String, filter: Option[String])

object IndexInfo {
  implicit val codec = AutomaticJsonCodecBuilder[IndexInfo]
}
