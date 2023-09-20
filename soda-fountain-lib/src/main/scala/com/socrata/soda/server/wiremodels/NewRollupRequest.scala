package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AllowMissing}

import com.socrata.soql.analyzer2._
import com.socrata.soql.stdlib.analyzer2.UserParameters

import com.socrata.soda.server.id.{RollupName, ResourceName}

case class NewRollupRequest(
  baseDataset: ResourceName,
  name: RollupName,
  soql: UnparsedFoundTables[metatypes.RollupMetaTypes],
  @AllowMissing("UserParameters.empty")
  userParameters: UserParameters,
  @AllowMissing("Nil")
  rewritePasses: Seq[Seq[rewrite.Pass]]
)

object NewRollupRequest {
  implicit val codec = AutomaticJsonCodecBuilder[NewRollupRequest]
}
