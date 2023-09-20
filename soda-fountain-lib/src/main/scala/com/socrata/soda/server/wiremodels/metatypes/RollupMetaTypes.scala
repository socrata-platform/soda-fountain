package com.socrata.soda.server.wiremodels.metatypes

import com.socrata.soql.analyzer2.MetaTypes
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLType, SoQLValue}

import com.socrata.soda.server.id.ResourceName

final abstract class RollupMetaTypes extends MetaTypes {
  type ResourceNameScope = Int
  type ColumnType = SoQLType
  type ColumnValue = SoQLValue
  type DatabaseTableNameImpl = ResourceName
  type DatabaseColumnNameImpl = ColumnName
}
