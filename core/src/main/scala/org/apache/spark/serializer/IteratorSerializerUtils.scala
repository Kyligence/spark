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

import java.io.{ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.io.ChunkedByteBufferInputStream



object IteratorSerializerUtils {

  def serialize[T: ClassTag](iterator: Iterator[T], size: Int): ByteBuffer = {

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bos = new ByteArrayOutputStream()
    val objOut = new ObjectOutputStream(codec.compressedOutputStream(bos))
    objOut.writeInt(size)
    while (iterator.hasNext) {
      objOut.writeBoolean(true)
      objOut.writeObject(iterator.next)
    }
    objOut.writeBoolean(false)
    objOut.flush()
    objOut.close()
    ByteBuffer.wrap(bos.toByteArray)

  }

  def deserialize[T: ClassTag](byteIterator: Iterator[ByteBuffer]): (Iterator[T], Int) = {

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ChunkedByteBufferInputStream(byteIterator, false)
    val ins = new ObjectInputStream((codec.compressedInputStream(bis)))
    val size = ins.readInt()
    val rows = new Iterator[T] {
      private var hasNextRow = ins.readBoolean()

      override def hasNext: Boolean = hasNextRow

      override def next(): T = {
        val row = ins.readObject().asInstanceOf[T]
        hasNextRow = ins.readBoolean()
        row
      }
    }
    
    (rows, size)

  }

}
