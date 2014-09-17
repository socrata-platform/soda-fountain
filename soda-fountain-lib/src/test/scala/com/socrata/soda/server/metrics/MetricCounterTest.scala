package com.socrata.soda.server.metrics

import java.util.concurrent.atomic.AtomicLong
import org.scalatest.{FunSpec, Matchers}

class MetricCounterTest extends FunSpec with Matchers {
  describe("A MetricCounter") {

    it("Should initialize correctly by default") {
      val counter = new MetricCounter()
      counter.get() should be (0)
    }

    it("Should initialize correctly when a starting value is specified") {
      val counter = new MetricCounter(new AtomicLong(4))
      counter.get() should be (4)
    }

    it("Should increment and decrement correctly") {
      val counter = new MetricCounter()
      counter.increment() should be (1)
      counter.increment() should be (2)
      counter.decrement() should be (1)
      counter.decrement() should be (0)
    }

    it("Should add correctly") {
      val counter = new MetricCounter()
      counter.add(1234) should be (1234)
      counter.add(4321) should be (5555)
    }
  }
}
