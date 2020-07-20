package com.socrata.soda.server

import java.util.concurrent.atomic.AtomicInteger

import org.slf4j.LoggerFactory

final class ThreadLimiter(consumerName: String, maxThreads: Int) {
  private val log = LoggerFactory.getLogger(classOf[ThreadLimiter])

  val usedThreads: AtomicInteger = new AtomicInteger(0)

  def withThreadpool[T](f: => T): T = {
    var response: Option[T] = None
    try {
      if (usedThreads.incrementAndGet() > maxThreads)
        throw TooManyThreadsException(consumerName)
      response = Some(f)
    } finally {
      usedThreads.decrementAndGet()
    }
    response.get
  }
}

case class TooManyThreadsException(consumerName: String) extends SodaInternalException(s"$consumerName is consuming too many threads.")
