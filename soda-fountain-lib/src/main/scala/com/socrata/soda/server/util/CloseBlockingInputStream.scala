package com.socrata.soda.server.util

import java.io.InputStream

class CloseBlockingInputStream(underlying: InputStream) extends InputStream {
  override def close(): Unit = {}
  override def read(): Int = underlying.read()
  override def read(buf: Array[Byte]): Int = underlying.read(buf)
  override def read(buf: Array[Byte], off: Int, len: Int) = underlying.read(buf, off, len)
  override def markSupported = underlying.markSupported
  override def mark(limit: Int) = underlying.mark(limit)
  override def reset() = underlying.reset()
  override def available() = underlying.available()
  override def skip(n: Long) = underlying.skip(n)
}
