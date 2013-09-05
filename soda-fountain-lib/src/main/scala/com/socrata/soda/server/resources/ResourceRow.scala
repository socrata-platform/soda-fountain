package com.socrata.soda.server.resources

import com.socrata.soda.server.id.ResourceName

case class ResourceRow() {
  case class service(resourceName: ResourceName, row: String) extends SodaResource
}
