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
  tables: UnparsedFoundTables[metatypes.QueryMetaTypes],
  @AllowMissing("Context.empty")
  context: Context,
  @AllowMissing("Nil")
  rewritePasses: Seq[Seq[Pass]],
  @AllowMissing("true")
  allowRollups: Boolean,
  @AllowMissing("false")
  preserveSystemColumns: Boolean,
  debug: Option[Debug],
  queryTimeoutMS: Option[Long]
)

object FoundTablesRequest {
  implicit val codec = AutomaticJsonCodecBuilder[FoundTablesRequest]
}
