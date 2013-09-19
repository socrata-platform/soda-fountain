package com.socrata.soda.server.util

import org.bouncycastle.crypto.engines.BlowfishEngine
import org.bouncycastle.crypto.params.KeyParameter
import java.nio.charset.StandardCharsets
import java.io.ByteArrayOutputStream
import org.apache.commons.codec.binary.Base64

// This implements CFB encryption/decryption with an IV derived from the
// contents of the message (the first eight bytes of its SHA1).  This is
// NOT DESIGNED FOR PERFECT SECURITY. This is OBFUSCATION ONLY.  The
// important properties are:
//   1. Identical messages will be obfuscated identically
//   2. Changes anywhere in the message will (with high probability)
//      produce a completely different obfuscation.
class ETagObfuscator(key: Array[Byte]) {
  private val bf = new BlowfishEngine
  bf.init(true, new KeyParameter(key))

  def nonce(out: Array[Byte], bs: Array[Byte]) {
    val hash = java.security.MessageDigest.getInstance("SHA1")
    val result = hash.digest(bs)
    var i = 0
    while(i < 8) {
      out(i) = result(i)
      i += 1
    }
  }

  def obfuscate(text: String): String = {
    val textBytes = text.getBytes(StandardCharsets.UTF_8)

    val baos = new ByteArrayOutputStream

    val lastBlock = new Array[Byte](8)
    nonce(lastBlock, textBytes)
    baos.write(lastBlock)

    var i = 0
    while(i < (textBytes.length & ~7)) {
      bf.processBlock(lastBlock, 0, lastBlock, 0)
      var j = 0
      while(j != 8) {
        lastBlock(j) = (lastBlock(j) ^ textBytes(i + j)).toByte
        j += 1
      }
      i += 8

      baos.write(lastBlock)
    }

    if(i != textBytes.length) {
      bf.processBlock(lastBlock, 0, lastBlock, 0)
      var j = 0
      while(i + j < textBytes.length) {
        lastBlock(j) = (lastBlock(j) ^ textBytes(i + j)).toByte
        j += 1
      }
      baos.write(lastBlock, 0, j)
    }

    Base64.encodeBase64URLSafeString(baos.toByteArray)
  }

  def deobfuscate(etag: String): String = {
    val bytes = Base64.decodeBase64(etag)
    if(bytes.length < 8) return ""

    val lastBlock = new Array[Byte](8)

    val baos = new ByteArrayOutputStream
    System.arraycopy(bytes, 0, lastBlock, 0, 8)

    var i = 8
    while(i < (bytes.length & ~7)) {
      bf.processBlock(lastBlock, 0, lastBlock, 0)
      var j = 0
      while(j != 8) {
        lastBlock(j) = (lastBlock(j) ^ bytes(i + j)).toByte
        j += 1
      }

      baos.write(lastBlock)

      lastBlock(0) = bytes(i + 0)
      lastBlock(1) = bytes(i + 1)
      lastBlock(2) = bytes(i + 2)
      lastBlock(3) = bytes(i + 3)
      lastBlock(4) = bytes(i + 4)
      lastBlock(5) = bytes(i + 5)
      lastBlock(6) = bytes(i + 6)
      lastBlock(7) = bytes(i + 7)

      i += 8
    }

    if(i != bytes.length) {
      bf.processBlock(lastBlock, 0, lastBlock, 0)
      var j = 0
      while(i + j < bytes.length) {
        lastBlock(j) = (lastBlock(j) ^ bytes(i + j)).toByte
        j += 1
      }
      baos.write(lastBlock, 0, j)
    }

    val deobfuscated = baos.toByteArray

    // re-hashing the deobfuscated contents to check against the first block
    // isn't necessary.  This is an etag, not an authentication code or message.
    // The only thing someone tampering with it can do is cause themselves to
    // fail to match.
    new String(deobfuscated, StandardCharsets.UTF_8)
  }
}
