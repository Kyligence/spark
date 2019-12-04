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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.util.AccumulatorV2

class ParquetMetricAccumulator extends AccumulatorV2[String, String] {

  private var res = ""

  /**
   * Returns if this accumulator is zero value or not. e.g. for a counter accumulator, 0 is zero
   * value; for a list accumulator, Nil is zero value.
   */
  override def isZero: Boolean = res.isEmpty

  /**
   * Creates a new copy of this accumulator.
   */
  override def copy(): AccumulatorV2[String, String] = {
    val newAccu = new ParquetMetricAccumulator
    newAccu.res = this.res
    newAccu
  }

  /**
   * Resets this accumulator, which is zero value. i.e. call `isZero` must
   * return true.
   */
  override def reset(): Unit = {
    res = ""
  }

  /**
   * Takes the inputs and accumulates.
   */
  override def add(v: String): Unit = {
    throw new UnsupportedOperationException
  }

  def setValue(newValue: String): Unit = {
    this.res = newValue
  }


  /**
   * Merges another same-type accumulator into this one and update its state, i.e. this should be
   * merge-in-place.
   */
  override def merge(other: AccumulatorV2[String, String]): Unit = {
  }

  /**
   * Defines the current value of this accumulator
   */
  override def value: String = res
}
