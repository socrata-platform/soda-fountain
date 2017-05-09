package com.socrata.soda.server.computation

import com.socrata.soda.server.id.ResourceName

import java.io.Closeable
import java.nio.charset.StandardCharsets

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.NodeCache


class ComputingGate(curator: CuratorFramework, path: String) extends (ResourceName => Boolean) with Closeable {
  val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[ComputingGate])
  val sfComputingEnabledForEnvironmentNodeCache = new NodeCache(curator, path + "/computation-enabled")
  val sfComputingEnabledForResourceNameNodeCache = new NodeCache(curator, path + "/computation-blacklist")

  def start(): Unit = {
    sfComputingEnabledForEnvironmentNodeCache.start(true)
    sfComputingEnabledForResourceNameNodeCache.start(true)
  }

  def close(): Unit = {
    sfComputingEnabledForEnvironmentNodeCache.close()
    sfComputingEnabledForResourceNameNodeCache.close()
  }

  // In the comments below we speak in terms of Soda Fountain's computation being _enabled_. This is because we want
  // to eventually disable it by changing (a) value(s) in Zookeeper, rather than simply enable the Secondary to do the
  // work instead--in other words, whether or not the Secondary will perform the computation is unrelated to whether
  // or not Soda Fountain will, and as such we want to wean Soda Fountain off of the computation racket by first
  // blacklisting some resources by ResourceName and eventually telling Soda Fountain it should never perform the
  // computation (by adding the string value "false" to the "/com.socrata/soda/soda-fountain/computation-enabled" path
  // in Zookeeper).
  def apply(resourceName: ResourceName): Boolean = {
    val falseAsBytes = "false".getBytes(StandardCharsets.UTF_8)

    // We want Soda Fountain to do the computation if...
    val sfComputingEnabledForEnvironment = Option(sfComputingEnabledForEnvironmentNodeCache.getCurrentData) match {
      // ...the "/com.socrata/soda/soda-fountain/computation-enabled" path exists and the data associated with the
      // node is not equal to the string "false"
      case Some(enabledForEnvironment) => !java.util.Arrays.equals(enabledForEnvironment.getData, falseAsBytes)
      // ...or if the "/com.socrata/soda/soda-fountain/computation-enabled" node does not exist at all.
      case None => true
    }

    // We want Soda Fountain to do the computation if the resourceName associated with the operation is not in the
    // resource name blacklist.
    val sfComputingEnabledForResourceName = Option(sfComputingEnabledForResourceNameNodeCache.getCurrentData) match {
      case Some(resourceNameBlacklist) => !new String(resourceNameBlacklist.getData, StandardCharsets.UTF_8).
        split(',').
        toSeq.
        map(new ResourceName(_)).
        contains(resourceName)
      case None => true
    }

    val logMessage = (sfComputingEnabledForEnvironment, sfComputingEnabledForResourceName) match {
      case (true, true) => "Performing computation in Soda Fountain; computation enabled for environment and " +
        s"""resource name "${resourceName.toString}" is not in the computation blacklist."""
      case (true, false) => "Not performing computation in Soda Fountain; computation enabled for environment but " +
        s"""resource name "${resourceName.toString}" is in the computation blacklist."""
      case (false, true) => "Not performing computation in Soda Fountain; computation not enabled for environment " +
        s"""but resource name "${resourceName.toString}" is not in the computation blacklist."""
      case (false, false) => "Not performing computation in Soda Fountain; computation not enabled for environment " +
        s"""and resource name "${resourceName.toString}" is in the computation blacklist."""
    }
    log.info(logMessage)

    // We only actually want Soda Fountain to do the computation if both of the above conditions are met.
    sfComputingEnabledForEnvironment && sfComputingEnabledForResourceName
  }
}
