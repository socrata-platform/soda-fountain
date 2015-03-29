package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._
import com.vividsolutions.jts.geom._

/**
 * Performance test for JsonColumnRep, specifically for the geometry types input and output.
 *
 * This is quick and dirty.  TODOs:
 * - Turn this into a performance regression using ScalaMeter
 * - include more tests
 */
object JsonColumnRepPerfBench extends App {
  val numPoints = 100000

  def randomLon: Double = scala.util.Random.nextDouble * 340d - 170d
  def randomLat: Double = scala.util.Random.nextDouble * 180d - 90d
  def factory = new GeometryFactory
  def randomPoint: Point = factory.createPoint(new Coordinate(randomLon, randomLat))

  println("Creating random points...")
  val sourcePoints = Array.fill(numPoints)(randomPoint)
  val sourceSoql = sourcePoints.map(SoQLPoint(_))

  def time(desc: String, f: => Unit): Unit = {
    println(s"Starting timing of $desc...")
    val start = System.currentTimeMillis
    f
    val msElapsed = System.currentTimeMillis - start
    println(s"$desc took ${msElapsed / 1000d} seconds")
  }

  time("JsonColumnRep SoQLPoint toJValue",
       sourceSoql.map(JsonColumnRep.forClientType(SoQLPoint).toJValue))

  val jValues = sourceSoql.map(JsonColumnRep.forClientType(SoQLPoint).toJValue)

  time("JsonColumnRep SoQLPoint fromJValue",
       jValues.map(JsonColumnRep.forClientType(SoQLPoint).fromJValue))
}