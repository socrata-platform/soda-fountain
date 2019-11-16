package com.socrata.soda.message

import java.io.Closeable


trait MessageProducer extends Closeable
{
  def start(): Unit
  def setServiceNames(serviceNames: Set[String]): Unit
  def shutdown(): Unit
  def send(message: Message, raw: Boolean): Unit
}

object NoOpMessageProducer extends MessageProducer {
  def start(): Unit = {}
  def setServiceNames(serviceNames: Set[String]): Unit = {}
  def shutdown(): Unit = {}
  def send(message: Message, raw: Boolean): Unit = {}
  def close(): Unit = {}
}
