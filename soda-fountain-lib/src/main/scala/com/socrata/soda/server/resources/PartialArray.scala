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
 * @param skipStartIndex - beginning index (0 base) of the range to skip
 * @param skipLength - size of the range to skip
 * @tparam T
 */
class PartialArray[T](underlying: Array[T], skipStartIndex: Int, skipLength: Int) {

  assert(skipStartIndex >= 0)
  assert(skipLength > 0 && skipStartIndex + skipLength <= underlying.length)

  private val offset = skipStartIndex + skipLength - 1

  def length: scala.Int = underlying.length - skipLength

  def apply(i : scala.Int) : T = {
    val realI = if (i < skipStartIndex) i else offset + i
    underlying(realI)
  }
}