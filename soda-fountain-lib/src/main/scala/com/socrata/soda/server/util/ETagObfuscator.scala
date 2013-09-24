package com.socrata.soda.server.util

import org.bouncycastle.crypto.engines.BlowfishEngine
import org.bouncycastle.crypto.params.KeyParameter
import java.nio.charset.StandardCharsets
import org.apache.commons.codec.binary.Base64
import com.socrata.http.server.util.{WeakEntityTag, StrongEntityTag, EntityTag}

trait ETagObfuscator {
  def obfuscate(text: EntityTag): EntityTag
  def deobfuscate(etag: EntityTag): EntityTag
}

object ETagObfuscator {
  def noop: ETagObfuscator = NoopEtagObfuscator
}

object NoopEtagObfuscator extends ETagObfuscator {
  def obfuscate(text: EntityTag): EntityTag = text
  def deobfuscate(etag: EntityTag): EntityTag = etag
}

// This implements CFB encryption/decryption with an IV derived from the
// contents of the message (the first eight bytes of its SHA1).  This is
// NOT DESIGNED FOR PERFECT SECURITY. This is OBFUSCATION ONLY.  The
// important properties are:
//   1. Identical messages will be obfuscated identically
//   2. Changes anywhere in the message will (with high probability)
//      produce a completely different obfuscation.
class BlowfishCFBETagObfuscator(key: Array[Byte]) extends ETagObfuscator {
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

  def obfuscate(tag: EntityTag): EntityTag = {
    val textBytes = tag.value.getBytes(StandardCharsets.UTF_8)
    val cryptBytes = new Array[Byte](textBytes.length + 8)

    nonce(cryptBytes, textBytes)

    var i = 0
    val limit = textBytes.length & ~7
    while(i < limit) {
      bf.processBlock(cryptBytes, i, cryptBytes, i + 8)
      var j = 0
      while(j != 8) {
        cryptBytes(i + j + 8) = (cryptBytes(i + j + 8) ^ textBytes(i + j)).toByte
        j += 1
      }
      i += 8
    }

    if(i != textBytes.length) {
      val lastBlock = new Array[Byte](8)
      bf.processBlock(cryptBytes, i, lastBlock, 0)
      var j = 0
      while(i + j < textBytes.length) {
        cryptBytes(i + j + 8) = (lastBlock(j) ^ textBytes(i + j)).toByte
        j += 1
      }
    }

    tag.map(_ => Base64.encodeBase64URLSafeString(cryptBytes))
  }

  def deobfuscate(etag: EntityTag): EntityTag = {
    val bytes = Base64.decodeBase64(etag.value)
    if(bytes.length < 8) return etag.map(_ => "")
    val textBytes = new Array[Byte](bytes.length - 8)

    var i = 0
    val limit = textBytes.length & ~7
    while(i < limit) {
      bf.processBlock(bytes, i, bytes, i)
      var j = 0
      while(j != 8) {
        textBytes(i + j) = (bytes(i + j) ^ bytes(i + j + 8)).toByte
        j += 1
      }
      i += 8
    }

    if(i != textBytes.length) {
      bf.processBlock(bytes, i, bytes, i)
      var j = 0
      while(i + j < textBytes.length) {
        textBytes(i + j) = (bytes(i + j) ^ bytes(i + j + 8)).toByte
        j += 1
      }
    }

    // re-hashing the deobfuscated contents to check against the first block
    // isn't necessary.  This is an etag, not an authentication code or message.
    // The only thing someone tampering with it can do is cause themselves to
    // fail to match.
    etag.map(_ => new String(textBytes, StandardCharsets.UTF_8))
  }
}
