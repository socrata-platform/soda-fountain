package com.socrata.soda.server.util

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean

/**
 * An Iterator that closes a managed resource when the base iterator runs out of items
 * @param baseIterator the base Iterator
 * @param resource the resource to close() when the base runs out of items
 */
class ManagedIterator[A](baseIterator: Iterator[A], resource: Closeable) extends Iterator[A] {
  val closed = new AtomicBoolean

  override def hasNext: Boolean = {
    if (baseIterator.hasNext) {
      true
    } else {
      if (!closed.get && closed.compareAndSet(false, true)) {
        resource.close()
      }
      false
    }
  }

  override def next: A = baseIterator.next
}