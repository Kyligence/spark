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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import org.apache.commons.collections.map.{AbstractReferenceMap, ReferenceMap}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging


private[spark] class BroadcastManager(
    val isDriver: Boolean,
    conf: SparkConf,
    securityManager: SecurityManager)
  extends Logging {

  val cleanQueryBroadcast = conf.getBoolean("spark.broadcast.autoClean.enabled", false)

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null
  var cachedBroadcast = new ConcurrentHashMap[String, ListBuffer[Long]]()

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  private def initialize() {
    synchronized {
      if (!initialized) {
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf, securityManager)
        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)

  private[spark] def currentBroadcastId: Long = nextBroadcastId.get()

  private[broadcast] val cachedValues = {
    new ReferenceMap(AbstractReferenceMap.HARD, AbstractReferenceMap.WEAK)
  }

  def cleanBroadCast(executionId: String): Unit = {
      if (cachedBroadcast.containsKey(executionId)) {
        cachedBroadcast.get(executionId)
          .foreach(broadcastId => unbroadcast(broadcastId, true, false))
        cachedBroadcast.remove(executionId)
      }
  }

  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, executionId: String): Broadcast[T] = {
    val broadcastId = nextBroadcastId.getAndIncrement()
    if (executionId != null && cleanQueryBroadcast) {
      if (cachedBroadcast.containsKey(executionId)) {
        cachedBroadcast.get(executionId) += broadcastId
      } else {
        val list = new scala.collection.mutable.ListBuffer[Long]
        list += broadcastId
        cachedBroadcast.put(executionId, list)
      }
    }
    broadcastFactory.newBroadcast[T](value_, isLocal, broadcastId)
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}
