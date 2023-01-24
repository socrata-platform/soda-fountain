package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.soda.server.id.RollupName
import com.socrata.soql.environment.ResourceName

case class RollupDatasetRelation(primaryDataset: ResourceName,name: RollupName,soql:String,secondaryDatasets:Set[ResourceName])

object RollupDatasetRelation{
  import com.socrata.soda.server.util.ResourceNameCodec._
  implicit val jCodec = AutomaticJsonCodecBuilder[RollupDatasetRelation]
}
