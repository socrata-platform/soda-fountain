package com.socrata.soda.server.resources

/**
 * Array like class that provides length and index.
 * But it skips b elements from a.
 *
 * [ 1, 2, 3, 4, 5, 6, 7 ]
 *     a=1
 *      |---b=4--|
 *
 *   produces
 *
 * [1, 6, 7]
 * @param underlying
 * @param a - beginning index (0 base) of the range to skip
 * @param b - size of the range to skip
 * @tparam T
 */
class PartialArray[T](underlying: Array[T], a: Int, b: Int) {

  private val offset = a + b - 1

  def length: scala.Int = underlying.length - b

  def apply(i : scala.Int) : T = {
    val realI = if (i < a) i else offset + i
    underlying(realI)
  }
}