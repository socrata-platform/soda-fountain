package com.socrata.soda.server.util

import java.io.{InputStream, IOException}

// An input stream that has an unbounded mark/reset capability.  If
// `mark` is called with a positive limit, data will be stored until
// the entire underlying stream is read or memory runs out.  Calling
// `mark` with a non-positive limit will discard that storage.
//
// Merely calling `reset` does _not_ discard the mark!
class UnboundedResettableInputStream(underlying: InputStream) extends InputStream {
  private sealed abstract class State extends InputStream {
    override final def close(): Unit = {
      underlying.close()
      state = new NeitherRecordingNorPlayingBack
    }
    override final def read(buf: Array[Byte]): Int = read(buf, 0, buf.length)
    override final def markSupported = true

    override def mark(limit: Int): Unit
    override def reset(): Unit

    override def read(): Int
    override def read(buf: Array[Byte], off: Int, len: Int): Int

    override def skip(n: Long): Long

    protected def notRecording(): Nothing = throw new Exception("Not recording");
  }

  private class NeitherRecordingNorPlayingBack extends State {
    override def available(): Int = underlying.available()

    override def mark(limit: Int): Unit =
      if(limit > 0) {
        state = new Recording(new Array[Byte](1024), 0)
      }

    override def reset(): Unit = notRecording()

    override def read(): Int = underlying.read()

    override def read(buf: Array[Byte], off: Int, len: Int): Int = underlying.read(buf, off, len)

    override def skip(n: Long): Long = underlying.skip(n)
  }

  private class Recording(var buf: Array[Byte], var end: Int) extends State {
    // Invariant: 0 <= end, end <= buf.len (i.e., there may or may not be space in the buffer)

    private def nextGrowth = buf.length
    private def freeSpace = buf.length - end
    private def isFull = freeSpace == 0

    private def ensureSpace(): Unit = {
      if(end == buf.length) {
        val newBuf = new Array[Byte](buf.length + nextGrowth)
        System.arraycopy(buf, 0, newBuf, 0, end)
        buf = newBuf
      }
    }

    override def available(): Int = {
      val bound = if(isFull) nextGrowth else freeSpace
      Math.min(underlying.available, bound)
    }

    override def mark(limit: Int) =
      if(limit > 0) {
        end = 0
      } else {
        state = new NeitherRecordingNorPlayingBack
      }

    override def reset(): Unit = {
      state = new Playback(buf, 0, end, true)
    }

    override def read(): Int = {
      ensureSpace()
      val b = underlying.read()
      if(b >= 0) {
        buf(end) = b.toByte
        end += 1
      }
      b
    }

    override def read(callerBuf: Array[Byte], off: Int, len: Int): Int = {
      if(callerBuf == null) throw new NullPointerException
      if(off < 0 || len < 0 || len > callerBuf.length - off) {
        throw new IndexOutOfBoundsException
      }

      if(len == 0) {
        return 0
      }

      ensureSpace()
      // Short reads are always allowed, so only read up to the free
      // space in our buffer
      val count = underlying.read(buf, end, Math.min(freeSpace, len))
      if(count > 0) {
        System.arraycopy(buf, end, callerBuf, off, count)
        end += count
      }
      count
    }

    override def skip(n: Long): Long = {
      var skipped = 0L

      while(skipped < n) {
        ensureSpace()
        val desired = Math.min(freeSpace, n - skipped).toInt // won't overflow, bounded above by freeSpace and below by 0
        val count = underlying.read(buf, end, desired)
        if(count > 0) {
          end += count
          skipped += count
        }
        if(count != desired) {
          // short skip from underlying; stop here
          return skipped
        }
      }

      skipped
    }
  }

  // Invariant: start >= 0, start < end, end <= buf.len (i.e., there
  // is data remaining to play back).
  // If recording is true, then the current mark is the start of the
  // buffer.
  private class Playback(var buf: Array[Byte], var start: Int, var end: Int, var recording: Boolean) extends State {
    private def remaining = end - start

    private def shift(): Unit = {
      if(start != 0) {
        val n = remaining
        System.arraycopy(buf, start, buf, 0, n)
        start = 0
        end = n
      }
    }

    private def nextState(): State =
      if(remaining == 0) {
        if(recording) {
          // everything in this buffer is still part of the record
          new Recording(buf, end)
        } else {
          new NeitherRecordingNorPlayingBack
        }
      } else {
        this
      }

    override def available(): Int = remaining

    override def mark(limit: Int): Unit =
      if(limit > 0) {
        shift() // discard everything in our buffer prior to the current start-position
        recording = true
      } else {
        recording = false
      }

    override def reset(): Unit = {
      if(recording) {
        start = 0
      } else {
        notRecording()
      }
    }

    override def read(): Int = {
      val ret = buf(start)
      start += 1
      state = nextState()
      ret & 0xff
    }

    override def read(callerBuf: Array[Byte], off: Int, len: Int): Int = {
      if(callerBuf == null) throw new NullPointerException
      if(off < 0 || len < 0 || len > callerBuf.length - off) {
        throw new IndexOutOfBoundsException
      }

      if(len == 0) {
        return 0
      }

      val count = Math.min(remaining, len)
      System.arraycopy(buf, start, callerBuf, off, count)
      start += count
      state = nextState()
      count
    }

    override def skip(n: Long): Long = {
      val r = remaining
      if(n > r) {
        if(recording) {
          // everything in this buffer is still a part of the record
          state = new Recording(buf, end)
        } else {
          state = new NeitherRecordingNorPlayingBack
        }
        buf = null // don't hold on to the buffer unnecessarily
        r + state.skip(n - r)
      } else {
        start += Math.max(0, n).toInt // won't overflow, just checked that it's no more than remaining
        state = nextState()
        n
      }
    }
  }

  private var state: State = new NeitherRecordingNorPlayingBack

  override def close(): Unit = state.close()
  override def available(): Int = state.available()
  override def read(): Int = state.read()
  override def read(buf: Array[Byte]): Int = state.read(buf)
  override def read(buf: Array[Byte], off: Int, len: Int): Int = state.read(buf, off, len)
  override def markSupported = state.markSupported
  override def mark(limit: Int) = state.mark(limit)
  override def reset(): Unit = state.reset()
  override def skip(n: Long): Long = state.skip(n)
}
