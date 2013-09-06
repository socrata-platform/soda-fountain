package com.socrata.soda.server.util

import java.util.concurrent.{Future, TimeUnit, Callable, ExecutorService}
import java.{util => ju}

class CloseableExecutorService(underlying: ExecutorService) extends ExecutorService {
  def close() {
    underlying.shutdown()
  }

  def shutdown() { underlying.shutdown() }

  def shutdownNow(): ju.List[Runnable] = underlying.shutdownNow()

  def isShutdown: Boolean = underlying.isShutdown

  def isTerminated: Boolean = underlying.isTerminated

  def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = underlying.awaitTermination(timeout, unit)

  def submit[T](task: Callable[T]): Future[T] = underlying.submit(task)

  def submit[T](task: Runnable, result: T): Future[T] = underlying.submit(task, result)

  def submit(task: Runnable): Future[_] = underlying.submit(task)

  def invokeAll[T](tasks: ju.Collection[_ <: Callable[T]]): ju.List[Future[T]] = underlying.invokeAll(tasks)

  def invokeAll[T](tasks: ju.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit): ju.List[Future[T]] = underlying.invokeAll(tasks, timeout, unit)

  def invokeAny[T](tasks: ju.Collection[_ <: Callable[T]]): T = underlying.invokeAny(tasks)

  def invokeAny[T](tasks: ju.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit): T = underlying.invokeAny(tasks, timeout, unit)

  def execute(command: Runnable) { underlying.execute(command) }
}
