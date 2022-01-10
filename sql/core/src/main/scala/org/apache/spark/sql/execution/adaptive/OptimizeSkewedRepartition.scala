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
package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.OptimizeSkewedJoin.{createSkewPartitionSpecs, getSizeInfo, isSkewed, medianSize, targetSize}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, REPARTITION_WITH_NUM, ShuffleExchangeExec, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf

object OptimizeSkewedRepartition extends CustomShuffleReaderRule {

  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REPARTITION_WITH_NUM)

  private val ensureRequirements = EnsureRequirements

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SKEW_REPARTITION_ENABLED)) {
      return plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case stage: ShuffleQueryStageExec => Seq(stage)
      case _ => plan.children.flatMap(collectShuffleStages)
    }

    val shuffleStages = collectShuffleStages(plan)
    if (shuffleStages.length == 1) {
      val optimizePlan = optimizeSkewRepartition(plan)
      val numShuffles = ensureRequirements.apply(optimizePlan).collect {
        case e: ShuffleExchangeExec => e
      }.length

      if (numShuffles > 0) {
        logDebug("OptimizeSkewedRepartition rule is not applied due" +
          " to additional shuffles will be introduced.")
        plan
      } else {
        optimizePlan
      }
    } else {
      plan
    }
  }

  private object ShuffleStage {
    def unapply(plan: SparkPlan): Option[ShuffleStageInfo] = plan match {
      case s: ShuffleQueryStageExec
        if s.mapStats.isDefined =>
        val mapStats = s.mapStats.get
        val sizes = mapStats.bytesByPartitionId
        val partitions = sizes.zipWithIndex.map {
          case (size, i) => CoalescedPartitionSpec(i, i + 1) -> size
        }
        Some(ShuffleStageInfo(s, mapStats, partitions))
      case _ => None
    }
  }

  def optimizeSkewRepartition(plan: SparkPlan): SparkPlan = plan.transformUp {
    case s@ShuffleStage(stageInfo: ShuffleStageInfo) =>
      val numPartitions = stageInfo.partitionsWithSizes.length
      // We use the median size of the original shuffle partitions to detect skewed partitions.
      val medSize = medianSize(stageInfo.mapStats)
      logDebug(
        s"""
           |Optimizing skew repartition.
           |Partitions size info:
           |${getSizeInfo(medSize, stageInfo.mapStats.bytesByPartitionId)}
        """.stripMargin)

      // We use the actual partition sizes to calculate target size, so that
      // the final data distribution is even (coalesced partitions + split partitions).
      val actualSizes = stageInfo.partitionsWithSizes.map(_._2)
      val targetSize0 = targetSize(actualSizes, medSize)
      val partitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
      var numSkewed = 0
      for (partitionIndex <- 0 until numPartitions) {
        val actualSize = actualSizes(partitionIndex)
        val isSkew = isSkewed(actualSize, medSize)
        val partSpec = stageInfo.partitionsWithSizes(partitionIndex)._1

        // A skewed partition should never be coalesced, but skip it here just to be safe.
        val parts = if (isSkew) {
          val reducerId = partSpec.startReducerIndex
          val skewSpecs = createSkewPartitionSpecs(
            stageInfo.mapStats.shuffleId, reducerId, targetSize0)
          if (skewSpecs.isDefined) {
            logDebug(s"Partition $partitionIndex " +
              s"(${FileUtils.byteCountToDisplaySize(actualSize)}) is skewed, " +
              s"split it into ${skewSpecs.get.length} parts.")
            numSkewed += 1
          }
          skewSpecs.getOrElse(Seq(partSpec))
        } else {
          Seq(partSpec)
        }

        for {
          sidePartition <- parts
        } {
          partitions += sidePartition
        }
      }

      logDebug(s"number of skewed partitions: num $numSkewed")
      if (numSkewed > 0) {
        val newPlan = CustomShuffleReaderExec(stageInfo.shuffleStage, partitions.toSeq)
        newPlan
      } else {
        s
      }
  }

}
