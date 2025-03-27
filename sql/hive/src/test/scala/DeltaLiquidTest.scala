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

import org.apache.spark.sql.{QueryTest, SparkSession}
class DeltaLiquidTest extends QueryTest {

  val metastore = "/home/lwz/devSrc/spark/metastore_db"
  val warehouse = "/home/lwz/devSrc/spark/spark-warehouse"

  val spark: SparkSession = {
    SparkSession.builder()
      .master("local[10]")
      .appName("DeltaLiquidTest")
                .enableHiveSupport()
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.stats.enabled", "true")
      .config("spark.databricks.delta.optimizeWrite.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "/home/lwz/Desktop/spark-events")
      .config("spark.databricks.delta.properties.defaults.targetFileSize=", "4mb")
      .config("spark.databricks.delta.optimize.maxFileSize", "4194304")
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$metastore;create=true")
      .getOrCreate()
  }

  test("test delta liquid table") {

    spark.sql("create database if not exists parquet_part")
    spark.sql("use parquet_part")
    spark.sql(
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS parquet_part.store_sales
        |(
        |     ss_sold_time_sk INT,
        |     ss_item_sk INT,
        |     ss_customer_sk INT,
        |     ss_cdemo_sk INT,
        |     ss_hdemo_sk INT,
        |     ss_addr_sk INT,
        |     ss_store_sk INT,
        |     ss_promo_sk INT,
        |     ss_ticket_number bigint,
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
        |     ss_net_profit DECIMAL(7,2)
        |)
        |USING PARQUET
        |TBLPROPERTIES (engine='Parquet')
        |PARTITIONED BY(ss_sold_date_sk INT)
        |LOCATION '/data/parquet-part/store_sales';
        |""".stripMargin)

    spark.sql("MSCK REPAIR TABLE parquet_part.store_sales")

    spark.sql(
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS parquet_part.date_dim
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
        |USING PARQUET
        |TBLPROPERTIES (engine='Parquet')
        |LOCATION '/data/parquet-part/date_dim';
        |""".stripMargin)

    stage()

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
    spark.sql(sql).show()
  }

  def stage(): Unit = {
    spark.sql("use default")
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
        |CLUSTER BY (ss_sold_date_sk, ss_item_sk)
        |LOCATION '/data/delta/store_sales';
        |""".stripMargin)
    spark.sql(
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS date_dim
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
        |USING DELTA
        |LOCATION '/data/delta/date_dim';
        |""".stripMargin)

    val salesCount = spark.sql("SELECT count(*) FROM store_sales").collect().head.getLong(0)
    val dateCount = spark.sql("SELECT count(*) FROM date_dim").collect().head.getLong(0)
    print(s"salesCount: $salesCount, dateCount: $dateCount")

    if (salesCount == 0) {
      spark.sql("INSERT INTO store_sales SELECT * FROM parquet_part.store_sales")
      spark.sql("optimize store_sales")
    }
    if (dateCount == 0) {
      spark.sql("INSERT INTO date_dim SELECT * FROM parquet_part.date_dim")
      spark.sql("optimize date_dim")
    }
  }


}
