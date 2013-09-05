package com.socrata.soda.server.wiremodels

import com.socrata.soda.server.errors.SodaError
import java.io.IOException

sealed abstract class ExtractResult[+T] {
  def map[U](f: T => U): ExtractResult[U]
  def flatMap[U](f: T => ExtractResult[U]): ExtractResult[U]
}

object ExtractResult {
  def sequence[A](es: Seq[ExtractResult[A]]): ExtractResult[Seq[A]] = {
    val result = Vector.newBuilder[A]
    es.foreach {
      case Extracted(a) => result += a
      case failure: ExtractFailure => return failure
    }
    Extracted(result.result())
  }
}

case class Extracted[T](value: T) extends ExtractResult[T] {
  def map[U](f: T => U): Extracted[U] = Extracted(f(value))
  def flatMap[U](f: T => ExtractResult[U]): ExtractResult[U] = f(value)
}

sealed abstract class ExtractFailure extends ExtractResult[Nothing] {
  def map[U](f: Nothing => U): this.type = this
  def flatMap[U](f: Nothing => ExtractResult[U]): this.type = this
}

case class IOProblem(error: IOException) extends ExtractFailure
case class RequestProblem(error: SodaError) extends ExtractFailure
