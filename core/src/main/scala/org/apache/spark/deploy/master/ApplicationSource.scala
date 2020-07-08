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

package org.apache.spark.deploy.master

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[master] class ApplicationSource(val application: ApplicationInfo) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "%s.%s.%s".format("application", application.desc.name,
    System.currentTimeMillis())

  metricRegistry.register(MetricRegistry.name("status"), new Gauge[String] {
    override def getValue: String = application.state.toString
  })

  metricRegistry.register(MetricRegistry.name("runtime_ms"), new Gauge[Long] {
    override def getValue: Long = application.duration
  })

  metricRegistry.register(MetricRegistry.name("cores"), new Gauge[Int] {
    override def getValue: Int = application.coresGranted
  })

  metricRegistry.register(MetricRegistry.name("cores_per_executor"), new Gauge[Int] {
    override def getValue: Int = application.desc.coresPerExecutor.getOrElse(1)
  })

  metricRegistry.register(MetricRegistry.name("max_cores"), new Gauge[Int] {
    override def getValue: Int = application.desc.maxCores.getOrElse(0)
  })

  metricRegistry.register(MetricRegistry.name("instances"), new Gauge[Int] {
    val maxCores: Int = application.desc.maxCores.getOrElse(0)
    val coresPerExecutor: Int = application.desc.coresPerExecutor.getOrElse(1)
    override def getValue: Int = maxCores / coresPerExecutor
  })

  metricRegistry.register(MetricRegistry.name("request_executors"), new Gauge[Int] {
    override def getValue: Int = application.executorLimit
  })

  metricRegistry.register(MetricRegistry.name("executors"), new Gauge[Int] {
    override def getValue: Int = application.executors.size
  })

}
