package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.util.{AutomaticJsonCodec, AllowMissing}

import com.socrata.soql.analyzer2._
import com.socrata.soql.stdlib.analyzer2.UserParameters

import com.socrata.soda.server.id.{RollupName, ResourceName}

@AutomaticJsonCodec
case class NewRollupRequest(
  baseDataset: ResourceName,
  name: RollupName,
  soql: UnparsedFoundTables[metatypes.RollupMetaTypes],
  @AllowMissing("UserParameters.empty")
  userParameters: UserParameters,
  @AllowMissing("Nil")
  rewritePasses: Seq[Seq[rewrite.AnyPass]]
)

object NewRollupRequest {
  @AutomaticJsonCodec
  case class Stored(
    soql: UnparsedFoundTables[metatypes.RollupMetaTypes],
    userParameters: UserParameters,
    rewritePasses: Seq[Seq[rewrite.AnyPass]]
  )
}
