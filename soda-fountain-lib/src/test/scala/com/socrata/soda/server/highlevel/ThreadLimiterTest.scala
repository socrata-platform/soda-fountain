package com.socrata.soda.server.highlevel

import java.util.concurrent.{CountDownLatch, ForkJoinPool, TimeUnit}
import com.socrata.soda.server.ThreadLimiter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class ThreadLimiterTest  extends AnyFunSuite with Matchers {
  import ThreadLimiterTest._

  test("Thread limiter should allow a number of threads below the limit") {
    val totalThreads = 5
    val fixture = testFixture()
    val latch = new CountDownLatch(totalThreads)

    for (_ <- 1 to totalThreads) {
      Future {
        fixture.withThreadpool {
          slowFunction(latch)
        }
      }
    }

    assert(latch.await(2, TimeUnit.SECONDS))
  }

  test("Thread limiter should reject threads over the limit") {
    val totalThreads = 6
    val fixture = testFixture()
    val latch = new CountDownLatch(totalThreads)

    for (_ <- 1 to totalThreads) {
      Future {
        fixture.withThreadpool {
          slowFunction(latch)
        }
      }
    }

    assert(!latch.await(2, TimeUnit.SECONDS))
    assert(latch.getCount == 1)
  }

  test("Thread limiter should release claimed threads on completion") {
    val totalThreads = 5
    val fixture = testFixture()
    val latch = new CountDownLatch(totalThreads)

    for (_ <- 1 to totalThreads) {
      Future {
        fixture.withThreadpool {
          slowFunction(latch)
        }
      }
    }

    assert(latch.await(2, TimeUnit.SECONDS))

    // Do it again to see if the claimed threads released
    val latch2 = new CountDownLatch(totalThreads)
    for (_ <- 1 to totalThreads) {
      Future {
        fixture.withThreadpool {
          slowFunction(latch2)
        }
      }
    }

    assert(latch2.await(2, TimeUnit.SECONDS))
  }


}

object ThreadLimiterTest {
  implicit val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(new ForkJoinPool(10))

  def slowFunction(latch: CountDownLatch) = {
    Thread.sleep(500)
    latch.countDown()
  }

  def testFixture() = new ThreadLimiter("TestClient", 5)
}
