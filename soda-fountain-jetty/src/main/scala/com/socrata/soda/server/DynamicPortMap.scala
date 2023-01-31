package com.socrata.soda.server

trait DynamicPortMap {
  private val intRx = "(\\d+)".r

  def hostPort(port: Int): Int = {
    Option(System.getenv(s"PORT_$port")) match {
      case Some(intRx(hostPort)) =>
        hostPort.toInt
      case _ => port
    }
  }
}
