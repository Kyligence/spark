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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.FileSourceScanExec

class DeltaLiquidTest extends QueryTest with TPCDSBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
         .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .set("spark.databricks.delta.stats.enabled", "true")
         .set("spark.databricks.delta.optimizeWrite.enabled", "true")
         .set("spark.databricks.delta.properties.defaults.targetFileSize=", "4mb")
         .set("spark.databricks.delta.optimize.maxFileSize", "4194304")
  }

  test("test delta liquid table") {
    spark.sql("create database if not exists liquid")
    spark.sql("use liquid")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS store_sales
        |(
        |     ss_sold_time_sk INT,
        |     ss_item_sk INT,
        |     ss_customer_sk INT,
        |     ss_cdemo_sk INT,
        |     ss_hdemo_sk INT,
        |     ss_addr_sk INT,
        |     ss_store_sk INT,
        |     ss_promo_sk INT,
        |     ss_ticket_number INT,
        |     ss_quantity INT,
        |     ss_wholesale_cost DECIMAL(7,2),
        |     ss_list_price DECIMAL(7,2),
        |     ss_sales_price DECIMAL(7,2),
        |     ss_ext_discount_amt DECIMAL(7,2),
        |     ss_ext_sales_price DECIMAL(7,2),
        |     ss_ext_wholesale_cost DECIMAL(7,2),
        |     ss_ext_list_price DECIMAL(7,2),
        |     ss_ext_tax DECIMAL(7,2),
        |     ss_coupon_amt DECIMAL(7,2),
        |     ss_net_paid DECIMAL(7,2),
        |     ss_net_paid_inc_tax DECIMAL(7,2),
        |     ss_net_profit DECIMAL(7,2),
        |     ss_sold_date_sk INT
        |)
        |USING DELTA
        |CLUSTER BY (ss_sold_date_sk, ss_item_sk);
        |""".stripMargin)
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS date_dim
        |(
        |     d_date_sk INT,
        |     d_date_id VARCHAR(16),
        |     d_date DATE,
        |     d_month_seq INT,
        |     d_week_seq INT,
        |     d_quarter_seq INT,
        |     d_year INT,
        |     d_dow INT,
        |     d_moy INT,
        |     d_dom INT,
        |     d_qoy INT,
        |     d_fy_year INT,
        |     d_fy_quarter_seq INT,
        |     d_fy_week_seq INT,
        |     d_day_name VARCHAR(9),
        |     d_quarter_name VARCHAR(6),
        |     d_holiday VARCHAR(1),
        |     d_weekend VARCHAR(1),
        |     d_following_holiday VARCHAR(1),
        |     d_first_dom INT,
        |     d_last_dom INT,
        |     d_same_day_ly INT,
        |     d_same_day_lq INT,
        |     d_current_day VARCHAR(1),
        |     d_current_week VARCHAR(1),
        |     d_current_month VARCHAR(1),
        |     d_current_quarter VARCHAR(1),
        |     d_current_year VARCHAR(1)
        |)
        |USING DELTA;
        |""".stripMargin)

    val sql =
      """
        |SELECT
        |  avg(ss_quantity) agg1,
        |  avg(ss_list_price) agg2,
        |  avg(ss_coupon_amt) agg3,
        |  avg(ss_sales_price) agg4
        |FROM store_sales join date_dim on ss_sold_date_sk = d_date_sk
        |where d_year = 2000
        |LIMIT 100
        |""".stripMargin
    val df = spark.sql(sql)
    val tableScans: Seq[FileSourceScanExec] = df.queryExecution.sparkPlan.collect {
      case scan : FileSourceScanExec => scan
    }

    assert(tableScans.size == 2)
    assert(tableScans.exists(_.clusterKeyFilters.nonEmpty))
  }


}
