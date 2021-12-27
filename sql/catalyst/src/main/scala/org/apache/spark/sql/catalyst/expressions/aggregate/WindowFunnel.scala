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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, IntegerType}


/**
 * @param windowLit window size in long
 * @param evtNumExpr number of events
 * @param eventTsCol event ts in long
 * @param evtConds expr to return event id (starting from 0)
 * @param dimValueExpr expr to return other dim values (关联属性)
 */
case class WindowFunnel(windowLit: Expression,
                        evtNumExpr: Expression,
                        eventTsCol: Expression,
                        evtConds: Expression,
                        dimValueExpr: Expression,
                        mutableAggBufferOffset: Int = 0,
                        inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Seq[Event]] with Serializable with Logging {

  def this(windowLit: Expression,
           evtNumExpr: Expression,
           eventTsCol: Expression,
           evtConds: Expression,
           dimValueExpr: Expression) = {
    this(windowLit, evtNumExpr, eventTsCol, evtConds, dimValueExpr, 0, 0)
  }

  lazy val window: Long = windowLit.eval().toString.toLong
  lazy val evtNum: Int = evtNumExpr.eval().toString.toInt

  override def createAggregationBuffer(): Seq[Event] = Seq[Event]()

  def toBoolean(expr: Expression, input: InternalRow): Boolean = {
    expr.eval(input).toString.toBoolean
//    new Cast(expr, BooleanType).eval(input).toString.toBoolean
  }

  def toInteger(expr: Expression, input: InternalRow): Int = {
    expr.eval(input).toString.toInt
//    new Cast(expr, IntegerType).eval(input).toString.toInt
  }

  def toLong(expr: Expression, input: InternalRow): Long = {
    expr.eval(input).toString.toLong
//    new Cast(expr, LongType).eval(input).toString.toLong
  }

  def toString(expr: Expression, input: InternalRow): String = {
    val raw = expr.eval(input)
    if (raw != null) {
      raw.toString
    } else {
      null
    }
//    val raw = new Cast(expr, StringType).eval(input)
//    if (raw != null) {
//      raw.toString
//    } else {
//      null
//    }
  }

  override def update(buffer: Seq[Event], input: InternalRow): Seq[Event] = {
    val evtId = toInteger(evtConds, input)
    if (evtId == -1) {
      return buffer
    }
    val ts = toLong(eventTsCol, input)
    val dimValue = toString(dimValueExpr, input)

    buffer :+ Event(evtId, ts, dimValue)
  }

  override def merge(buffer: Seq[Event],
                     input: Seq[Event]): Seq[Event] = {
    buffer ++ input
  }

  override def eval(buffer: Seq[Event]): Any = {
    val grouped = buffer.groupBy(e => e.dim)
    val nullDimEvents = grouped.getOrElse(null, Seq())

//    logDebug("events " + buffer)
//    logDebug("group by dim" + nullDimEvents)

    grouped.map(e => {
      if (e._1 == null) {
        e
      } else {
        // append null dim events to other dims as they can concat to any sequences
        (e._1, e._2 ++ nullDimEvents)
      }
    }).map(e => longestSeqId(e._2)).max
  }

  private def longestSeqId(events: Seq[Event]): Int = {
    val sorted = events.sortBy(e => e.ts)

    val timestamps = Array.fill[Long](evtNum)(-1)
    sorted.foreach(e => {
      if (e.eid == 0) {
        timestamps(e.eid) = e.ts
      } else if (timestamps(e.eid -1) > -1 && timestamps(e.eid -1) + window >= e.ts) {
        timestamps(e.eid) = e.ts
      }

      if (timestamps.last > -1) {
        return timestamps.length
      }
    })

    timestamps.lastIndexWhere(ts => ts > -1) + 1
  }

  override def serialize(buffer: Seq[Event]): Array[Byte] = {
    SerializeUtil.serialize(buffer)
  }

  override def deserialize(storageFormat: Array[Byte]): Seq[Event] = {
    SerializeUtil.deserialize(storageFormat)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] =
    windowLit :: eventTsCol :: evtNumExpr :: evtConds :: dimValueExpr :: Nil
}

case class Event(eid: Int, ts: Long, dim: String)

object SerializeUtil {
  import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

  def writeEvent(oss: ObjectOutputStream, event: Event): Unit = {
    oss.writeInt(event.eid)
    oss.writeLong(event.ts)
    if (event.dim == null) {
      oss.writeBoolean(true)
    } else {
      oss.writeBoolean(false)
      oss.writeUTF(event.dim)
    }
  }

  def readEvent(ois: ObjectInputStream): Event = {
    val id = ois.readInt()
    val ts = ois.readLong()
    if (ois.readBoolean()) {
      Event(id, ts, null)
    } else {
      Event(id, ts, ois.readUTF())
    }
  }

  def serialize(events: Seq[Event]): Array[Byte] = {
    try {
      val baos = new ByteArrayOutputStream
      val oos = new ObjectOutputStream(baos)
      oos.writeInt(events.length)
      events.foreach(e => writeEvent(oos, e))
//      oos.writeObject(events)
      oos.flush()
      val bytes = baos.toByteArray
      bytes
    } catch {
      case e: Exception =>
        throw new RuntimeException("serialize failed", e)
    }
  }

  def deserialize(bytes: Array[Byte]): Seq[Event] = {
    try {
      val bais = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bais)
      val size = ois.readInt()
      (0 until size).map(_ => readEvent(ois))
    } catch {
      case e: Exception =>
        throw new RuntimeException("deserialize failed", e)
    }
  }
}
