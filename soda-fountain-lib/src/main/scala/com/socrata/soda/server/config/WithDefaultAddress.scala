package com.socrata.soda.server.config

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.curator.x.discovery.ServiceInstanceBuilder
import com.rojoma.json.ast.JString

object WithDefaultAddress extends (Config => Config) {
  def apply(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if(ifaces.isEmpty) config
    else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig = ConfigFactory.parseString("com.socrata.soda-fountain.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }
}
