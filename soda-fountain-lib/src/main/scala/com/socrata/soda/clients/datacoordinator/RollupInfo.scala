package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.soda.server.id.RollupName

case class RollupInfo(name: RollupName, soql: String)

object RollupInfo {
  implicit val codec = AutomaticJsonCodecBuilder[RollupInfo]
}
