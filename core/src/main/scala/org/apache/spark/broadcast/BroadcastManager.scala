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

package org.apache.spark.broadcast

import java.util.{Collections, Objects}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import org.apache.commons.collections.map.{AbstractReferenceMap, ReferenceMap}

import org.apache.spark.SparkConf
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.internal.Logging

private case class TimedExecutionId(
    executionId: String,
    timeCreated: Long = System.currentTimeMillis()) {

  override def hashCode(): Int = Objects.hash(executionId)

  override def equals(obj: Any): Boolean =
    obj match {
      case that: TimedExecutionId => this.executionId == that.executionId
      case _ => false
    }
}

private[spark] class BroadcastManager(
    val isDriver: Boolean, conf: SparkConf) extends Logging {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null

  private val cleanQueryBroadcast =
    conf.getBoolean("spark.broadcast.autoClean.enabled", defaultValue = false)
  private val broadcastDefaultTTL =
    conf.getTimeAsMs("spark.broadcast.autoClean.defaultTTL", "30m")
  private val executionLocks = new ConcurrentHashMap[TimedExecutionId, Object]()
  private val cachedBroadcast = new ConcurrentHashMap[TimedExecutionId, ListBuffer[Long]]()

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  private def initialize(): Unit = {
    synchronized {
      if (!initialized) {
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf)
        initialized = true
      }
    }
  }

  def stop(): Unit = {
    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)

  private[spark] def currentBroadcastId: Long = nextBroadcastId.get()

  private[broadcast] val cachedValues =
    Collections.synchronizedMap(
      new ReferenceMap(AbstractReferenceMap.HARD, AbstractReferenceMap.WEAK)
        .asInstanceOf[java.util.Map[Any, Any]]
    )

  def cleanBroadcast(executionId: String): Unit = {
    val timedExecutionId = TimedExecutionId(executionId, 0)
    if (cachedBroadcast.containsKey(timedExecutionId)) {
      val lock = executionLocks.computeIfAbsent(timedExecutionId, _ => new Object())
      lock.synchronized {
        try {
          val bids = cachedBroadcast.get(timedExecutionId)
          bids.foreach(broadcastId =>
            unbroadcast(broadcastId, removeFromDriver = true, blocking = false))
          cachedBroadcast.remove(timedExecutionId)
          if (log.isDebugEnabled()) {
            log.debug(
              s"Finally Clean broadcasts for executionId=${executionId}" +
                s" and size=${bids.length} and bids=${bids.mkString(",")}")
          }
        } catch {
          case e: Throwable => logError(
            s"Error while cleaning broadcasts for executionId=${executionId}", e)
        }
      }
      executionLocks.remove(timedExecutionId)
    }
  }

  def cleanOutdatedBroadcasts(currentExecutionIds: Set[java.lang.Long]): Unit = {
    if (cleanQueryBroadcast) {
      val now = System.currentTimeMillis()
      cachedBroadcast.keySet().forEach { timedExecutionId =>
        if (now - timedExecutionId.timeCreated > broadcastDefaultTTL
            && !currentExecutionIds.contains(timedExecutionId.executionId.toLong)) {
          log.debug(s"Clean outdated broadcasts for executionId=${timedExecutionId.executionId}")
          cleanBroadcast(timedExecutionId.executionId)
        }
      }
    }
  }

  def newBroadcast[T: ClassTag](
      value_ : T,
      isLocal: Boolean,
      executionId: String): Broadcast[T] = {
    val bid = nextBroadcastId.getAndIncrement()
    if (executionId != null && cleanQueryBroadcast) {
      val timedExecutionId = TimedExecutionId(executionId)
      val lock = executionLocks.computeIfAbsent(timedExecutionId, _ => new Object())
      lock.synchronized {
        val bids = cachedBroadcast.get(timedExecutionId)
        if (bids != null) {
          bids += bid
          log.debug(
            s"Record broadcasts for executionId=${executionId} and size=${bids.length}" +
              s" and bid=${bid}")
        } else {
          val list = new scala.collection.mutable.ListBuffer[Long]
          list += bid
          cachedBroadcast.put(timedExecutionId, list)
          log.debug(s"New broadcasts for executionId=${executionId} and bid=${bid}")
        }
      }
    }
    value_ match {
      case pb: PythonBroadcast =>
        // SPARK-28486: attach this new broadcast variable's id to the PythonBroadcast,
        // so that underlying data file of PythonBroadcast could be mapped to the
        // BroadcastBlockId according to this id. Please see the specific usage of the
        // id in PythonBroadcast.readObject().
        pb.setBroadcastId(bid)

      case _ => // do nothing
    }
    broadcastFactory.newBroadcast[T](value_, isLocal, bid)
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}
