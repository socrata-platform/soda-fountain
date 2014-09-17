package com.socrata.soda.server.metrics

import java.util.concurrent.atomic.AtomicLong

class MetricCounter(value: AtomicLong) {

  /** *
    * Default constructor
    */
  def this() = this(new AtomicLong())

  /** *
    * Increments the count by 1, then returns the new value
    * @return Value of the counter after incrementing
    */
  def get(): Long = value.get()

  /** *
    * Increments the count by 1, then returns the new value
    * @return Value of the counter after incrementing
    */
  def increment(): Long = value.incrementAndGet()

  /** *
    * Decrements the count by 1, then returns the new value
    * @return Value of the counter after decrementing
    */
  def decrement(): Long = value.decrementAndGet()

  /** *
    * Increments the count by toAdd, then returns the new value
    * @return Value of the counter after adding toAdd
    */
  def add(toAdd: Long): Long = value.addAndGet(toAdd)
}
