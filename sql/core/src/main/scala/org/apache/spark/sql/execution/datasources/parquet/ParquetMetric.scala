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

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

class ParquetMetric extends Serializable {
  val _pageReadHeaderTime = new LongAccumulator
  val _pageReadHeaderCnt = new LongAccumulator

  val _footerReadTime = new LongAccumulator
  val _footerReadCnt = new LongAccumulator

  val _groupReadTime = new LongAccumulator
  val _groupReadCnt = new LongAccumulator

  val _pageReadTime = new LongAccumulator
  val _pageReadCnt = new LongAccumulator

  val _filteredGroupReadTime = new LongAccumulator
  val _filteredGroupReadCnt = new LongAccumulator

  val _pageReadUncompressBytes = new LongAccumulator
  val _pageReadDecompressBytes = new LongAccumulator
  val _pageReadDecompressTime = new LongAccumulator

  val _totalPages = new LongAccumulator

  val _colPageIndexReadCnt = new LongAccumulator
  val _colPageIndexReadTime = new LongAccumulator

  val _offsetPageIndexReadCnt = new LongAccumulator
  val _offsetPageIndexReadTime = new LongAccumulator

  val _totalTime = new LongAccumulator

  val nameToAccums: mutable.LinkedHashMap[String, LongAccumulator] = mutable.LinkedHashMap(
    ParquetMetric.PARQUET_PAGE_READ_HEADER_TIME -> _pageReadHeaderTime,
    ParquetMetric.PARQUET_PAGE_READ_HEADER_CNT -> _pageReadHeaderCnt,
    ParquetMetric.PARQUET_FOOTER_READ_TIME -> _footerReadTime,
    ParquetMetric.PARQUET_FOOTER_READ_CNT -> _footerReadCnt,
    ParquetMetric.PARQUET_GROUP_READ_TIME -> _groupReadTime,
    ParquetMetric.PARQUET_GROUP_READ_CNT -> _groupReadCnt,
    ParquetMetric.PARQUET_PAGE_READ_TIME -> _pageReadTime,
    ParquetMetric.PARQUET_PAGE_READ_CNT -> _pageReadCnt,
    ParquetMetric.PARQUET_FILTERED_GROUP_READ_TIME -> _filteredGroupReadTime,
    ParquetMetric.PARQUET_FILTERED_GROUP_READ_CNT -> _filteredGroupReadCnt,
    ParquetMetric.PARQUET_PAGE_READ_UNCOMPRESS_BYTES -> _pageReadUncompressBytes,
    ParquetMetric.PARQUET_PAGE_READ_DECOMPRESS_BYTES -> _pageReadDecompressBytes,
    ParquetMetric.PARQUET_PAGE_READ_DECOMPRESS_TIME -> _pageReadDecompressTime,
    ParquetMetric.PARQUET_TOTAL_PAGES -> _totalPages,
    ParquetMetric.PARQUET_COL_PAGE_INDEX_READ_CNT -> _colPageIndexReadCnt,
    ParquetMetric.PARQUET_COL_PAGE_INDEX_READ_TIME -> _colPageIndexReadTime,
    ParquetMetric.PARQUET_OFFSET_PAGE_INDEX_READ_CNT -> _offsetPageIndexReadCnt,
    ParquetMetric.PARQUET_OFFSET_PAGE_INDEX_READ_TIME -> _offsetPageIndexReadTime,
    ParquetMetric.PARQUET_TOTAL_TIME -> _totalTime
  )

  private[spark] def register(sc: SparkContext): Unit = {
    nameToAccums.foreach {
      case (name, acc) => acc.register(sc, name = Some(name), countFailedValues = true)
    }
  }

  def isRegistered(): Boolean = {
    nameToAccums.values.forall(_.isRegistered)
  }
}

object ParquetMetric {
  val PARQUET_PAGE_READ_HEADER_TIME = "parquetPageReadHeaderTime"
  val PARQUET_PAGE_READ_HEADER_CNT = "parquetPageReadHeaderCnt"
  val PARQUET_FOOTER_READ_TIME = "parquetFooterReadTime"
  val PARQUET_FOOTER_READ_CNT = "parquetFooterReadCnt"
  val PARQUET_GROUP_READ_TIME = "parquetGroupReadTime"
  val PARQUET_GROUP_READ_CNT = "parquetGroupReadCnt"
  val PARQUET_PAGE_READ_TIME = "parquetPageReadTime"
  val PARQUET_PAGE_READ_CNT = "parquetPageReadCnt"
  val PARQUET_FILTERED_GROUP_READ_TIME = "parquetFilteredGroupReadTime"
  val PARQUET_FILTERED_GROUP_READ_CNT = "parquetFilteredGroupReadCnt"
  val PARQUET_PAGE_READ_UNCOMPRESS_BYTES = "parquetPageReadUncompressBytes"
  val PARQUET_PAGE_READ_DECOMPRESS_BYTES = "parquetPageReadDecompressBytes"
  val PARQUET_PAGE_READ_DECOMPRESS_TIME = "parquetPageReadDecompressTime"
  val PARQUET_TOTAL_PAGES = "parquetTotalPages"
  val PARQUET_COL_PAGE_INDEX_READ_CNT = "parquetColPageIndexReadCnt"
  val PARQUET_COL_PAGE_INDEX_READ_TIME = "parquetColPageIndexReadTime"
  val PARQUET_OFFSET_PAGE_INDEX_READ_CNT = "parquetOffsetPageIndexReadCnt"
  val PARQUET_OFFSET_PAGE_INDEX_READ_TIME = "parquetOffsetPageIndexReadTime"
  val PARQUET_TOTAL_TIME = "parquetTotalTime"
}
