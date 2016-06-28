package com.socrata.soda.server.computation

import java.io.Closeable
import java.nio.charset.StandardCharsets

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.NodeCache

class ComputingGate(curator: CuratorFramework, path: String) extends (() => Boolean) with Closeable {
  val nc = new NodeCache(curator, path + "/computation-enabled")

  def start(): Unit = {
    nc.start(true)
  }

  def close(): Unit = {
    nc.close()
  }

  def apply(): Boolean = {
    val d = nc.getCurrentData
    d == null || !java.util.Arrays.equals(d.getData, falseBytes)
  }

  val falseBytes = "false".getBytes(StandardCharsets.UTF_8)
}
