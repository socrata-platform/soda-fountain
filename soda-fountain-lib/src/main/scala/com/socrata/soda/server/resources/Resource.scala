package com.socrata.soda.server.resources

import com.socrata.soda.server.id.ResourceName

case class Resource() {
  case class service(resourceName: ResourceName) extends SodaResource
  case class rowService(resourceName: ResourceName, rowId: String) extends SodaResource
}
