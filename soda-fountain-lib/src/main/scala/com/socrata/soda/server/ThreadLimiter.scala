package com.socrata.soda.server

import java.util.concurrent.Semaphore

import org.slf4j.LoggerFactory

final class ThreadLimiter(consumerName: String, maxThreads: Int) {
  private val log = LoggerFactory.getLogger(classOf[ThreadLimiter])

  private val semaphore = new Semaphore(maxThreads)

  def withThreadpool[T](f: => T): T = {
    if(semaphore.tryAcquire()) {
      try {
        f
      } finally {
        semaphore.release()
      }
    } else {
      throw TooManyThreadsException(consumerName)
    }
  }
}

case class TooManyThreadsException(consumerName: String) extends SodaInternalException(s"$consumerName is consuming too many threads.")
