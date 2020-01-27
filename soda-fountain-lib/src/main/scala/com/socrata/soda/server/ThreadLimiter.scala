package com.socrata.soda.server

import java.util.concurrent.atomic.AtomicInteger

import org.slf4j.Logger

trait ThreadLimiter {
  val maxThreads: Option[Int] = None
  val log: Logger
  val consumerName: String

  val usedThreads: AtomicInteger = new AtomicInteger(0)

  def withThreadpool[T](f: => T): T = {
    maxThreads match {
      case Some(maxThreads) =>
        limitThreads(maxThreads, usedThreads, f)
      case None =>
        log.error(s"Trying to use threadpool without setting a maximum thread count")
        f
    }
  }

  def limitThreads[T](maxThreads: Int, threadCounter: AtomicInteger, f: => T): T = {
    var response: Option[T] = None
    try {
      if (threadCounter.incrementAndGet() > maxThreads)
        throw TooManyThreadsException(consumerName)
      response = Some(f)
    } finally {
      threadCounter.decrementAndGet()
    }
    response.get
  }
}

case class TooManyThreadsException(consumerName: String) extends SodaInternalException(s"$consumerName is consuming too many threads.")
