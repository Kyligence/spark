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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.Utils

import scala.io.Source

object TaoboParseBenchmark extends SqlBasedBenchmark with Logging {

  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setMaster(System.getProperty("spark.sql.test.master", "local[1]"))
      .setAppName("test-sql-context")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", System.getProperty("spark.sql.shuffle.partitions", "4"))
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")

    SparkSession.builder.config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val tableNames = Seq(
      //"kylin_view_dws_dim_union_chat_follow_flow",
      "kylin_view_ads_fact_day_org_kpi_wide"
    )

    // scalastyle:off nonascii
    val dryRunSQls = Seq(
      // "q17-店铺招募-Kylin.sql",
      "q06-线上线下.sql"
    )
    // scalastyle:on nonascii

    val rowCount = 1
    val executedCnt = 600

    val benchmark =
      new Benchmark(s"Spark Parse $rowCount rows", rowCount, output = output)

    val path = "/home/chang/SourceCode/gluten/backends-clickhouse/src/test/resources/poc/taobo";

    def schemaFileName(name: String): String = {
      s"$path/schema/${name}.ddl"
    }

    def queryFileName(name: String): String = {
      s"$path/queries/${name}"
    }

    dryRunSQls
      .map(name =>
          (name, Utils.tryWithResource(Source.fromFile(queryFileName(name))("UTF-8")) {
            source => source.mkString}))
      .foreach {
        case (name, sql) =>
          benchmark.addCase(name, executedCnt) {
            _ =>
              val y = spark.sql(sql).queryExecution.sparkPlan
              require(y.isInstanceOf[SparkPlan])
          }
      }

    Utils.tryWithSafeFinally {
      tableNames
        .map(schemaFileName)
        .map(file => Utils.tryWithResource(Source.fromFile(file)("UTF-8")) {
          source => source.mkString})
        .foreach(spark.sql)

      benchmark.run()
    } {
      tableNames.foreach {
        tableName =>
          spark.sql(s"""
                       |DROP TABLE IF EXISTS $tableName PURGE
                       |""".stripMargin)
      }
    }
  }
}
