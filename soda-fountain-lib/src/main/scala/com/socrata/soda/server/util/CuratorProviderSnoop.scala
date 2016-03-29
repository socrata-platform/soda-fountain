package com.socrata.soda.server.util

import scala.collection.JavaConverters._
import java.io.Closeable

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache

// TODO: Move this into curator-utils
class CuratorProviderSnoop(curatorClient: CuratorFramework, discoveryRoot: String, serviceName: String) extends (() => Set[String]) with Closeable {
  private val prefix = serviceName + "."
  private val nodeCache = new PathChildrenCache(curatorClient, discoveryRoot, false)

  def start(): Unit = {
    nodeCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
  }

  def close(): Unit = {
    nodeCache.close()
  }

  def apply(): Set[String] = {
    nodeCache.getCurrentData.asScala.flatMap { cd =>
      val p = cd.getPath
      val lastSlash = p.lastIndexOf('/')
      val service = p.substring(lastSlash + 1)
      if(service.startsWith(prefix)) List(service.substring(prefix.length))
      else Nil
    } (scala.collection.breakOut)
  }
}
