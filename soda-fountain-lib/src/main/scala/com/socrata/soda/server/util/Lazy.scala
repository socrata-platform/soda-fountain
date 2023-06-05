package com.socrata.soda.server.util

final class Lazy[T] private (candidate: () => T) {
  lazy val get = candidate()

  override def toString = get.toString
}

object Lazy {
  def apply[T](t: => T) =
    new Lazy(() => t)
}
