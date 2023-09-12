package org.apache.spark.serializer

import java.io.{ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.io.ChunkedByteBufferInputStream

import scala.reflect.ClassTag

object IteratorSerializerUtils {

  def serialize[T: ClassTag](iterator: Iterator[T]): ByteBuffer = {

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bos = new ByteArrayOutputStream()
    val objOut = new ObjectOutputStream(codec.compressedOutputStream(bos))
    while (iterator.hasNext) {
      objOut.writeBoolean(true)
      objOut.writeObject(iterator.next)
    }
    objOut.writeBoolean(false)
    objOut.flush()
    objOut.close()
    ByteBuffer.wrap(bos.toByteArray)

  }

  def deserialize[T: ClassTag](byteIterator: Iterator[ByteBuffer]): Iterator[T] = {

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ChunkedByteBufferInputStream(byteIterator, false)
    val ins = new ObjectInputStream((codec.compressedInputStream(bis)))
    new Iterator[T] {
      private var hasNextRow = ins.readBoolean()

      override def hasNext: Boolean = hasNextRow

      override def next(): T = {
        val row = ins.readObject().asInstanceOf[T]
        hasNextRow = ins.readBoolean()
        row
      }
    }

  }

}
