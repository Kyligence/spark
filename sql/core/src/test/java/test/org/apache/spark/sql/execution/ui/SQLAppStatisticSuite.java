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

package test.org.apache.spark.sql.execution.ui;

import org.apache.spark.sql.execution.ui.SQLAppStatistic;
import org.junit.Test;

public class SQLAppStatisticSuite {

  @Test
  public void testSkewnessAndKurtosis() {
    double[] arr1 = {60, 80, 130, 110, 70, 2020, 20, 77, 98, 330, 220, 177, 323, 200, 100};
    assert (SQLAppStatistic.skewness(arr1) > SQLAppStatistic.STANDARD_SKEWNESS
            && SQLAppStatistic.kurtosis(arr1) > SQLAppStatistic.STANDARD_KURTOSIS);

    double[] arr2 = {60, 70, 30, 55, 80, 37, 43, 28, 58, 26, 73, 41, 39, 63, 28};
    assert (!(SQLAppStatistic.skewness(arr2) > SQLAppStatistic.STANDARD_SKEWNESS
            && SQLAppStatistic.kurtosis(arr2) > SQLAppStatistic.STANDARD_KURTOSIS));
  }
}
