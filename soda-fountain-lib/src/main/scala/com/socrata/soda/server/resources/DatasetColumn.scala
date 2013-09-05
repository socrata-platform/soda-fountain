package com.socrata.soda.server.resources

import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName

case class DatasetColumn() {
  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource
}
