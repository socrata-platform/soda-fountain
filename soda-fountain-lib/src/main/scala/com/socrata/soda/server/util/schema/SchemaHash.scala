package com.socrata.soda.server.util.schema

import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.ColumnRecordLike
import java.security.MessageDigest
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Comparator
import com.socrata.soql.types.SoQLType

object SchemaHash {
  private val hexDigit = "0123456789abcdef".toCharArray

  private def hexString(xs: Array[Byte]) = {
    val cs = new Array[Char](xs.length * 2)
    var i = 0
    while(i != xs.length) {
      val dst = i << 1
      cs(dst) = hexDigit((xs(i) >> 4) & 0xf)
      cs(dst+1) = hexDigit(xs(i) & 0xf)
      i += 1
    }
    new String(cs)
  }

  def computeHash(locale: String, pk: ColumnId, columns: TraversableOnce[(ColumnId, SoQLType)]): String = {
    val sha1 = MessageDigest.getInstance("SHA-1")

    sha1.update(locale.getBytes(UTF_8))
    sha1.update(255.toByte)

    sha1.update(pk.underlying.getBytes(UTF_8))
    sha1.update(255.toByte)

    val cols = columns.toArray
    java.util.Arrays.sort(cols, new Comparator[(ColumnId, SoQLType)] {
      val o = Ordering[ColumnId]
      def compare(a: (ColumnId, SoQLType), b: (ColumnId, SoQLType)) =
        o.compare(a._1, b._1)
    })

    for((id, typ) <- cols) {
      sha1.update(id.underlying.getBytes(UTF_8))
      sha1.update(255.toByte)
      sha1.update(typ.name.caseFolded.getBytes(UTF_8))
      sha1.update(255.toByte)
    }

    hexString(sha1.digest())
  }

  def computeHash(locale: String, pk: ColumnId, columns: Seq[ColumnRecordLike]): String = {
    computeHash(locale, pk, columns.iterator.map { cr => (cr.id, cr.typ) })
  }
}
