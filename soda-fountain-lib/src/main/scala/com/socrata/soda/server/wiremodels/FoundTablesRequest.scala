package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AllowMissing}

import com.socrata.soql.analyzer2.UnparsedFoundTables
import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2.rewrite.Pass
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.stdlib.analyzer2.Context
import com.socrata.soql.sql.Debug

import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.ResourceName

case class FoundTablesRequest(
  tables: UnparsedFoundTables[FoundTablesRequest.MetaTypes],
  @AllowMissing("Context.empty")
  context: Context,
  @AllowMissing("Nil")
  rewritePasses: Seq[Seq[Pass]],
  @AllowMissing("false")
  preserveSystemColumns: Boolean,
  debug: Option[Debug]
)

object FoundTablesRequest {
  implicit val codec = AutomaticJsonCodecBuilder[FoundTablesRequest]

  final abstract class MetaTypes extends analyzer2.MetaTypes {
    type ResourceNameScope = Int
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type DatabaseTableNameImpl = (ResourceName, Stage)
    type DatabaseColumnNameImpl = ColumnName
  }
}