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

package org.apache.spark.sql.execution.ui;

/**
 * https://www.itl.nist.gov/div898/handbook/eda/section3/eda35b.htm
 */
public class SQLAppStatistic {

  public static final double STANDARD_SKEWNESS = 1.0d;
  public static final double STANDARD_KURTOSIS = 1.0d;

  private SQLAppStatistic() {
  }

  // mean of data.
  static double mean(double[] arr, int n) {

    double sum = 0;

    for (int i = 0; i < n; i++) {
      sum = sum + arr[i];
    }

    return sum / n;
  }

  // deviation of data.
  static double standardDeviation(double[] arr,
                                  int n) {
    double sum = 0;
    double miu = mean(arr, n);

    for (int i = 0; i < n; i++) {
      double delta = arr[i] - miu;
      sum += Math.pow(delta, 2);
    }

    return Math.sqrt(sum / n);
  }

  public static double skewness(double[] arr) {
    int n = arr.length;
    double sum = 0;
    double miu = mean(arr, n);
    for (double x : arr) {
      double delta = x - miu;
      sum += Math.pow(delta, 3);
    }

    double sigma = standardDeviation(arr, n);
    return sum / (n * Math.pow(sigma, 3));
  }

  public static double kurtosis(double[] arr) {
    int n = arr.length;
    double sum = 0;
    double miu = mean(arr, n);

    for (double x : arr) {
      double delta = x - miu;
      sum += Math.pow(delta, 4);
    }

    double sigma = standardDeviation(arr, n);
    return (sum / (n * Math.pow(sigma, 4))) - 3;
  }
}
