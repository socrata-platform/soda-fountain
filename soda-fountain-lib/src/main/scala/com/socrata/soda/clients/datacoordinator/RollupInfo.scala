package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.soda.server.id.RollupName
import com.socrata.soda.server.highlevel.RollupHelper

case class RollupInfo(name: RollupName, soql: String) {
  def isNewAnalyzer = RollupHelper.isNewAnalyzerSoql(soql)
}

object RollupInfo {
  implicit val codec = AutomaticJsonCodecBuilder[RollupInfo]
}
