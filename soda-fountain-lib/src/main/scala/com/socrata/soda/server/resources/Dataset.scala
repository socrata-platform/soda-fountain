package com.socrata.soda.server.resources

import com.socrata.soda.server.id.ResourceName

case class Dataset() {
  case class service(resourceName: ResourceName) extends SodaResource
}
