package com.socrata.soda.server.config

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.typesafe.config.Config

class ConfigClass(config: Config, root: String) {
  private def p(x: String) = root + "." + x
  def getInt(key: String) = config.getInt(p(key))
  def getString(key: String) = config.getString(p(key))
  def getStringList(key: String) = config.getStringList(p(key)).asScala
  def getDuration(key: String) = config.getMilliseconds(p(key)).longValue.millis
  def getConfig[T](key: String, decoder: (Config, String) => T) = decoder(config, p(key))
}
