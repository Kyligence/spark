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
package org.apache.spark.serializer

import java.io.{ByteArrayOutputStream, DataInput, DataInputStream, DataOutput, DataOutputStream, IOException}
import java.nio.ByteBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferInputStream


private[spark] trait IteratorItem {
  @throws(classOf[IOException])
  def writeExternalToDataOutput(out: DataOutput): Unit

  @throws(classOf[IOException])
  def readExternalFromDataOutput(in: DataInput): Unit

}

object IteratorSerializerUtils {

  def serialize[T <: IteratorItem](iterator: Iterator[T], size: Int): ByteBuffer = {

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(codec.compressedOutputStream(bos))
    out.writeInt(size)
    var classNameFilled = false
    while (iterator.hasNext) {
      out.writeBoolean(true)
      val row = iterator.next();
      if (!classNameFilled) {
        val className = row.getClass.getCanonicalName
        out.writeInt(className.length)
        out.writeChars(className)
        classNameFilled = true
      }
      row.writeExternalToDataOutput(out)
    }
    out.writeBoolean(false)
    out.flush()
    out.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  def deserialize[T <: IteratorItem](byteIterator: Iterator[ByteBuffer]): (Iterator[T], Int) = {
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ChunkedByteBufferInputStream(byteIterator, false)
    val ins = new DataInputStream((codec.compressedInputStream(bis)))
    val size = ins.readInt()
    val rows = new Iterator[T] {
      private var hasNextRow = ins.readBoolean()
      private var clazz : Class[_] = _
      if(hasNextRow) {
        val classNameLength = ins.readInt()
        val name : StringBuffer = new StringBuffer()
        for( i <- 1 to classNameLength) {
          name.append(ins.readChar())
        }
        clazz = Utils.classForName(name.toString)
      }
      override def hasNext: Boolean = hasNextRow

      override def next(): T = {
        val row = clazz.newInstance().asInstanceOf[T]
        row.readExternalFromDataOutput(ins)
        hasNextRow = ins.readBoolean()
        row
      }
    }
    (rows, size)
  }

}
