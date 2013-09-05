package com.socrata.soda.server.id

import com.socrata.soql.environment.AbstractName

class ResourceName(s: String) extends AbstractName(s){
  protected def hashCodeSeed: Int = -795755684
}
