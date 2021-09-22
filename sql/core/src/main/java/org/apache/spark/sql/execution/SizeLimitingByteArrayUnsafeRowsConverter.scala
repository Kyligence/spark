/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.{NextIterator, Utils}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

private[spark] class SizeLimitingByteArrayUnsafeRowsConverter(
                                                               maxCollectSize: Option[Long]
                                                             ) extends Logging with Serializable {
  private var totalUncompressedResultSize = 0L

  /**
    * Packing the UnsafeRows into byte array for faster serialization.
    * The byte arrays are in the following format:
    * [size] [bytes of UnsafeRow] [size] [bytes of UnsafeRow] ... [-1]
    *
    * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
    * compressed.
    */
  def encodeUnsafeRows(
                        n: Int = -1,
                        unsafeRows: Iterator[InternalRow],
                        takeFromEnd: Boolean): Iterator[(Long, ChunkedByteBuffer)] = {

    var count = 0
    val buffer = new Array[Byte](4 << 10)  // 4K
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val cbbos = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
    val out = new DataOutputStream(codec.compressedOutputStream(cbbos))

    if (takeFromEnd && n > 0) {
      // To collect n from the last, we should anyway read everything with keeping the n.
      // Otherwise, we don't know where is the last from the iterator.
      var last: Seq[UnsafeRow] = Seq.empty[UnsafeRow]
      val slidingIter = unsafeRows.map(_.copy()).sliding(n)
      while (slidingIter.hasNext) { last = slidingIter.next().asInstanceOf[Seq[UnsafeRow]] }
      var i = 0
      count = last.length
      while (i < count) {
        val row = last(i)
        ensureTotalSizeIsBelowLimit(row.getSizeInBytes)
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        i += 1
      }
    } else {
      // `iter.hasNext` may produce one row and buffer it, we should only call it when the
      // limit is not hit.
      while ((n < 0 || count < n) && unsafeRows.hasNext) {
        val row = unsafeRows.next().asInstanceOf[UnsafeRow]
        ensureTotalSizeIsBelowLimit(row.getSizeInBytes)
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        count += 1
      }
    }
    out.writeInt(-1)
    out.flush()
    out.close()
    Iterator((count, cbbos.toChunkedByteBuffer))
  }

  /**
   * Decodes the byte arrays back to UnsafeRows and put them into buffer.
   */
  def decodeUnsafeRows(nFields: Int, bytes: ChunkedByteBuffer): Iterator[InternalRow] = {
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val cbbis = bytes.toInputStream()
    val ins = new DataInputStream(codec.compressedInputStream(cbbis))

    new NextIterator[InternalRow] {
      private var sizeOfNextRow = ins.readInt()
      private def _next(): InternalRow = {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(nFields)
        row.pointTo(bs, sizeOfNextRow)
        sizeOfNextRow = ins.readInt()
        row
      }

      override def getNext(): InternalRow = {
        if (sizeOfNextRow >= 0) {
          try {
            _next()
          } catch {
            case t: Throwable if ins != null =>
              ins.close()
              throw t
          }
        } else {
          finished = true
          null
        }
      }
      override def close(): Unit = ins.close()
    }
  }

  private def ensureTotalSizeIsBelowLimit(sizeOfNextRow: Int): Unit = {
    totalUncompressedResultSize += sizeOfNextRow
    maxCollectSize match {
      case Some(maxSize) => if (totalUncompressedResultSize > maxSize) {
        val msg = s"Total size of uncompressed results " +
          s"(${Utils.bytesToString(totalUncompressedResultSize)}) " +
          s"is bigger than the limit of (${Utils.bytesToString(maxSize)})"
        logError(msg)
        throw new SparkException(msg)
      }
      case _ =>
    }
  }
}
