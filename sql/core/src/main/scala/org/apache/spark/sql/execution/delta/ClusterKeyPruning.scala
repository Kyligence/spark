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

package org.apache.spark.sql.execution.delta

import com.fasterxml.jackson.core.{JsonFactoryBuilder, JsonToken}
import com.fasterxml.jackson.core.json.JsonReadFeature

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.json.CreateJacksonParser
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.actions.DomainMetadata
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.util.Utils

object ClusterKeyPruning extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // Do not rewrite subqueries.
    case s: Subquery if s.correlated => plan
    case _ => prune(plan)
  }

  private def prune(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      case j@Join(Filter(_: DynamicPruningSubquery, _), _, _, _, _) => j
      case j@Join(_, Filter(_: DynamicPruningSubquery, _), _, _, _) => j
      case j@Join(left, right, joinType, Some(condition), hint) =>
        var newLeft = left
        var newRight = right

        // extract the left and right keys of the join condition
        val (leftKeys, rightKeys) = j match {
          case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _, _, _) => (lkeys, rkeys)
          case _ => (Nil, Nil)
        }

        // checks if two expressions are on opposite sides of the join
        def fromDifferentSides(x: Expression, y: Expression): Boolean = {
          def fromLeftRight(x: Expression, y: Expression) =
            !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
              !y.references.isEmpty && y.references.subsetOf(right.outputSet)

          fromLeftRight(x, y) || fromLeftRight(y, x)
        }

        splitConjunctivePredicates(condition).foreach {
          case EqualTo(a: Expression, b: Expression)
            if fromDifferentSides(a, b) =>
            val (l, r) = if (a.references.subsetOf(left.outputSet) &&
              b.references.subsetOf(right.outputSet)) {
              a -> b
            } else {
              b -> a
            }

            // there should be a cluster table and a filter on the dimension table,
            // otherwise the pruning will not trigger
            var filterableScan = getFilterableDeltaScan(l, left)
            if (filterableScan.isDefined && canPruneLeft(joinType) &&
              hasSelectiveFilter(right)) {
              newLeft = insertPredicate(l, newLeft, Seq(r), right, rightKeys, filterableScan.get)
            } else {
              filterableScan = getFilterableDeltaScan(r, right)
              if (filterableScan.isDefined && canPruneRight(joinType) &&
                hasSelectiveFilter(left)) {
                newRight = insertPredicate(r, newRight, Seq(l), left, leftKeys, filterableScan.get)
              }
            }
          case _ =>
        }
        Join(newLeft, newRight, joinType, Some(condition), hint)
    }
  }

  // cluster key needs to be join key
  def getFilterableDeltaScan(a: Expression, plan: LogicalPlan): Option[LogicalPlan] = {
    val srcInfo: Option[(Expression, LogicalPlan)] = findExpressionAndTrackLineageDown(a, plan)
    srcInfo.flatMap {
      case (resExp, l: LogicalRelation) =>
        val clusterColumns = getClusterCols(l.relation)
        val joinKeys = resExp.references.map(_.name).toSet
        if (joinKeys.subsetOf(clusterColumns.toSet)) {
          return Some(l)
        } else {
          None
        }
      case _ => None
    }
  }

  def getClusterCols(relation: BaseRelation): Seq[String] = {
    var domainMetadata: Seq[DomainMetadata] = Nil
    relation match {
      case fs: HadoopFsRelation =>
        fs.location match {
          case index: TahoeLogFileIndex =>
            domainMetadata = index.getSnapshot.domainMetadata
          case index: PreparedDeltaFileIndex =>
            domainMetadata = index.preparedScan.scannedSnapshot.domainMetadata
          case _ =>
        }
      case _ =>
    }
    val clusterInfo = domainMetadata.find(_.domain == "delta.clustering").map(d => d.configuration)
    if (clusterInfo.isEmpty) Nil else parseClusterCols(clusterInfo.get)
  }

  private def parseClusterCols(clusterInfo: String): Seq[String] = {
    val jsonFactory = new JsonFactoryBuilder()
      // The two options below enabled for Hive compatibility
      .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
      .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
      .build()
    val clusterCols = scala.collection.mutable.Set[String]()
    Utils.tryWithResource(CreateJacksonParser.string(jsonFactory, clusterInfo)) {
      parser => {
        while (parser.nextToken() != null) {
          if (parser.getCurrentToken == JsonToken.FIELD_NAME
            && parser.currentName() == "clusteringColumns") {
            parser.nextToken()
            while (parser.nextToken() != JsonToken.END_ARRAY) {
              parser.nextToken()
              clusterCols.add(parser.getValueAsString())
              parser.nextToken()
            }
          }
        }
      }
    }
    clusterCols.toSeq
  }

  private def insertPredicate(
    pruningKey: Expression,
    pruningPlan: LogicalPlan,
    filteringKeys: Seq[Expression],
    filteringPlan: LogicalPlan,
    joinKeys: Seq[Expression],
    partScan: LogicalPlan): LogicalPlan = {
    require(filteringKeys.size == 1,
      "Only one filtering key is supported for cluster key pruning")
    val index = joinKeys.indexOf(filteringKeys.head)

//    val filterKey = filteringKeys.head.asInstanceOf[Attribute].name
//    val spark = SparkSession.active
//    val res = Dataset.ofRows(spark, filteringPlan)
//                     .groupBy().agg(filterKey -> "min", filterKey -> "max").collect()
//
//    val minVal = res.head.get(0)
//    val maxVal = res.head.get(1)

    lazy val hasBenefit = true
    if (hasBenefit) {
      // insert a DynamicPruning wrapper to identify the subquery during query planning
//      Filter(
//        And(
//          GreaterThanOrEqual(pruningKey, Literal(minVal)),
//          LessThanOrEqual(pruningKey, Literal(maxVal))),
//        pruningPlan)
      Filter(
        DynamicPruningSubquery(
          pruningKey,
          filteringPlan,
          joinKeys,
          index,
          conf.dynamicPartitionPruningReuseBroadcastOnly || !hasBenefit),
        pruningPlan)
    } else {
      // abort dynamic partition pruning
      pruningPlan
    }
  }

  private def pruningHasBenefit(
    partExpr: Expression,
    partPlan: LogicalPlan,
    otherExpr: Expression,
    otherPlan: LogicalPlan): Boolean = {

    // get the distinct counts of an attribute for a given table
    def distinctCounts(attr: Attribute, plan: LogicalPlan): Option[BigInt] = {
      plan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
    }

    // the default filtering ratio when CBO stats are missing, but there is a
    // predicate that is likely to be selective
    val fallbackRatio = conf.dynamicPartitionPruningFallbackFilterRatio
    // the filtering ratio based on the type of the join condition and on the column statistics
    val filterRatio = (partExpr.references.toList, otherExpr.references.toList) match {
      // filter out expressions with more than one attribute on any side of the operator
      case (leftAttr :: Nil, rightAttr :: Nil)
        if conf.dynamicPartitionPruningUseStats =>
        // get the CBO stats for each attribute in the join condition
        val partDistinctCount = distinctCounts(leftAttr, partPlan)
        val otherDistinctCount = distinctCounts(rightAttr, otherPlan)
        val availableStats = partDistinctCount.isDefined && partDistinctCount.get > 0 &&
          otherDistinctCount.isDefined
        if (!availableStats) {
          fallbackRatio
        } else if (partDistinctCount.get.toDouble <= otherDistinctCount.get.toDouble) {
          // there is likely an estimation error, so we fallback
          fallbackRatio
        } else {
          1 - otherDistinctCount.get.toDouble / partDistinctCount.get.toDouble
        }
      case _ => fallbackRatio
    }

    val estimatePruningSideSize = filterRatio * partPlan.stats.sizeInBytes.toFloat
    val overhead = calculatePlanOverhead(otherPlan)
    estimatePruningSideSize > overhead
  }

  private def calculatePlanOverhead(plan: LogicalPlan): Float = {
    val (cached, notCached) = plan.collectLeaves().partition(p => p match {
      case _: InMemoryRelation => true
      case _ => false
    })
    val scanOverhead = notCached.map(_.stats.sizeInBytes).sum.toFloat
    val cachedOverhead = cached.map {
      case m: InMemoryRelation if m.cacheBuilder.storageLevel.useDisk &&
        !m.cacheBuilder.storageLevel.useMemory =>
        m.stats.sizeInBytes.toFloat
      case m: InMemoryRelation if m.cacheBuilder.storageLevel.useDisk =>
        m.stats.sizeInBytes.toFloat * 0.2
      case m: InMemoryRelation if m.cacheBuilder.storageLevel.useMemory =>
        0.0
    }.sum.toFloat
    scanOverhead + cachedOverhead
  }


  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.exists {
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }
  }

  private def hasSelectiveFilter(plan: LogicalPlan): Boolean = {
    !plan.isStreaming && hasSelectivePredicate(plan)
  }

}


