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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.sql.TypedImperativeAggregateSuite.TypedMax
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, ImplicitCastInputTypes, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{TypedImperativeAggregate, WindowFunnel}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class TypedImperativeAggregateSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val random = new java.util.Random()

  private val data = (0 until 1000).map { _ =>
    (random.nextInt(10), random.nextInt(100))
  }

  test("aggregate with object aggregate buffer") {
    val agg = new TypedMax(BoundReference(0, IntegerType, nullable = false))

    val group1 = (0 until data.length / 2)
    val group1Buffer = agg.createAggregationBuffer()
    group1.foreach { index =>
      val input = InternalRow(data(index)._1, data(index)._2)
      agg.update(group1Buffer, input)
    }

    val group2 = (data.length / 2 until data.length)
    val group2Buffer = agg.createAggregationBuffer()
    group2.foreach { index =>
      val input = InternalRow(data(index)._1, data(index)._2)
      agg.update(group2Buffer, input)
    }

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    assert(mergeBuffer.value == data.map(_._1).max)
    assert(agg.eval(mergeBuffer) == data.map(_._1).max)

    // Tests low level eval(row: InternalRow) API.
    val row = new GenericInternalRow(Array(mergeBuffer): Array[Any])

    // Evaluates directly on row consist of aggregation buffer object.
    assert(agg.eval(row) == data.map(_._1).max)
  }

  test("supports SpecificMutableRow as mutable row") {
    val aggregationBufferSchema = Seq(IntegerType, LongType, BinaryType, IntegerType)
    val aggBufferOffset = 2
    val buffer = new SpecificInternalRow(aggregationBufferSchema)
    val agg = new TypedMax(BoundReference(ordinal = 1, dataType = IntegerType, nullable = false))
      .withNewMutableAggBufferOffset(aggBufferOffset)

    agg.initialize(buffer)
    data.foreach { kv =>
      val input = InternalRow(kv._1, kv._2)
      agg.update(buffer, input)
    }
    assert(agg.eval(buffer) == data.map(_._2).max)
  }

  test("dataframe aggregate with object aggregate buffer, should not use HashAggregate") {
    val df = data.toDF("a", "b")
    val max = TypedMax($"a".expr)

    // Always uses SortAggregateExec
    val sparkPlan = df.select(Column(max.toAggregateExpression())).queryExecution.sparkPlan
    assert(!sparkPlan.isInstanceOf[HashAggregateExec])
  }

  test("dataframe aggregate with object aggregate buffer, no group by") {
    val df = data.toDF("key", "value").coalesce(2)
    val query = df.select(typedMax($"key"), count($"key"), typedMax($"value"), count($"value"))
    val maxKey = data.map(_._1).max
    val countKey = data.size
    val maxValue = data.map(_._2).max
    val countValue = data.size
    val expected = Seq(Row(maxKey, countKey, maxValue, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, non-nullable aggregator") {
    val df = data.toDF("key", "value").coalesce(2)

    // Test non-nullable typedMax
    val query = df.select(typedMax(lit(null)), count($"key"), typedMax(lit(null)),
      count($"value"))

    // typedMax is not nullable
    val maxNull = Int.MinValue
    val countKey = data.size
    val countValue = data.size
    val expected = Seq(Row(maxNull, countKey, maxNull, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, nullable aggregator") {
    val df = data.toDF("key", "value").coalesce(2)

    // Test nullable nullableTypedMax
    val query = df.select(nullableTypedMax(lit(null)), count($"key"), nullableTypedMax(lit(null)),
      count($"value"))

    // nullableTypedMax is nullable
    val maxNull = null
    val countKey = data.size
    val countValue = data.size
    val expected = Seq(Row(maxNull, countKey, maxNull, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregation with object aggregate buffer, input row contains null") {

    val nullableData = (0 until 1000).map {id =>
      val nullableKey: Integer = if (random.nextBoolean()) null else random.nextInt(100)
      val nullableValue: Integer = if (random.nextBoolean()) null else random.nextInt(100)
      (nullableKey, nullableValue)
    }

    val df = nullableData.toDF("key", "value").coalesce(2)
    val query = df.select(typedMax($"key"), count($"key"), typedMax($"value"),
      count($"value"))
    val maxKey = nullableData.map(_._1).filter(_ != null).max
    val countKey = nullableData.map(_._1).count(_ != null)
    val maxValue = nullableData.map(_._2).filter(_ != null).max
    val countValue = nullableData.map(_._2).count(_ != null)
    val expected = Seq(Row(maxKey, countKey, maxValue, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, with group by") {
    val df = data.toDF("value", "key").coalesce(2)
    val query = df.groupBy($"key").agg(typedMax($"value"), count($"value"), typedMax($"value"))
    val expected = data.groupBy(_._2).toSeq.map { group =>
      val (key, values) = group
      val valueMax = values.map(_._1).max
      val countValue = values.size
      Row(key, valueMax, countValue, valueMax)
    }
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, empty inputs, no group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.select(typedMax($"a"), count($"a"), typedMax($"b"), count($"b")),
      Seq(Row(Int.MinValue, 0, Int.MinValue, 0)))
  }

  test("dataframe aggregate with object aggregate buffer, empty inputs, with group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.groupBy($"b").agg(typedMax($"a"), count($"a"), typedMax($"a")),
      Seq.empty[Row])
  }

  test("TypedImperativeAggregate should not break Window function") {
    val df = data.toDF("key", "value")
    // OVER (PARTITION BY a ORDER BY b ROW BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    val w = Window.orderBy("value").partitionBy("key").rowsBetween(Long.MinValue, 0)

    val query = df.select(sum($"key").over(w), typedMax($"key").over(w), sum($"value").over(w),
      typedMax($"value").over(w))

    val expected = data.groupBy(_._1).toSeq.flatMap { group =>
      val (key, values) = group
      val sortedValues = values.map(_._2).sorted

      var outputRows = Seq.empty[Row]
      var i = 0
      while (i < sortedValues.size) {
        val unboundedPrecedingAndCurrent = sortedValues.slice(0, i + 1)
        val sumKey = key * unboundedPrecedingAndCurrent.size
        val maxKey = key
        val sumValue = unboundedPrecedingAndCurrent.sum
        val maxValue = unboundedPrecedingAndCurrent.max

        outputRows :+= Row(sumKey, maxKey, sumValue, maxValue)
        i += 1
      }

      outputRows
    }
    checkAnswer(query, expected)
  }

  private def typedMax(column: Column): Column = {
    val max = TypedMax(column.expr, nullable = false)
    Column(max.toAggregateExpression())
  }

  private def nullableTypedMax(column: Column): Column = {
    val max = TypedMax(column.expr, nullable = true)
    Column(max.toAggregateExpression())
  }

  private def checkWindowAnswer(df: DataFrame, expected: Seq[(Int, Int)]): Unit = {
    df.show(false)
    checkAnswer(df, expected.toDF())
  }

  test("test window funnel") {
    val colNames = Seq("uid", "eid", "dim1", "dim2", "ts")

    val df1 = Seq(
      //"uid", "eid", "dim1", "dim2", "ts"
      // single sequence
      (901, 1, null, null, 1)
      ,(901, 2, null, null, 2)
      ,(901, 3, null, null, 3)
      ,(901, 4, null, null, 4)
      ,(901, 5, null, null, 5)
      // single sequence (out of window)
      ,(902, 1, null, null, 1)
      ,(902, 2, null, null, 2)
      ,(902, 3, null, null, 100)
      ,(902, 4, null, null, 101)
      ,(902, 5, null, null, 102)
    ).toDF(colNames: _*)
    df1.createOrReplaceTempView("events")
    checkWindowAnswer(spark.sql("select uid, window_funnel(10, 5, ts, eid - 1, null) from events group by uid"), Seq((901, 5), (902, 2)))

    val df2 = Seq(
      //"uid", "eid", "dim1", "dim2", "ts"
      // multiple sequence
      (901, 1, null, null, 1)
      ,(901, 2, null, null, 2)
      ,(901, 1, null, null, 3)
      ,(901, 3, null, null, 4)
      ,(901, 1, null, null, 5)
      ,(901, 2, null, null, 6)
      ,(901, 4, null, null, 7)
      ,(901, 5, null, null, 11)
    ).toDF(colNames: _*)
    df2.createOrReplaceTempView("events")
    checkWindowAnswer(spark.sql("select uid, window_funnel(10, 5, ts, eid - 1, null) from events group by uid"), Seq((901, 5)))

    val df3 = Seq(
      //"uid", "eid", "dim1", "dim2", "ts"
      // multiple sequence with dim
      (901, 1, "CN", null, 1)
      ,(901, 2, null, null, 2)
      ,(901, 1, null, null, 3)
      ,(901, 3, null, "CN", 4)
      ,(901, 1, null, null, 5)
      ,(901, 2, null, null, 6)
      ,(901, 4, null, "CN", 7)
      ,(901, 5, null, null, 8)
      // multiple sequence with dim
      ,(902, 1, "CN", null, 1)
      ,(902, 2, null, null, 2)
      ,(902, 1, null, null, 3)
      ,(902, 3, null, "CN", 4)
      ,(902, 1, null, null, 5)
      ,(902, 2, null, null, 6)
      ,(902, 4, null, "US", 7)
      ,(902, 5, null, null, 8)
    ).toDF(colNames: _*)
    df3.createOrReplaceTempView("events")
    checkWindowAnswer(spark.sql(
      "select uid, window_funnel(10, 5, ts, eid - 1, " +
        "case when eid = 1 then dim1 " +
        "when eid = 2 then null " +
        "when eid = 3 then dim2 " +
        "when eid = 4 then dim2 " +
        "else null end) from events group by uid"),
      Seq((901, 5), (902, 3)))
  }

  test("test window funnel radnom") {
    val events = (0 until 1000000).map { _ =>
      (random.nextInt(100000) + 1, // uid
        random.nextInt(5) + 1, // evt id 1-5
        "dim_" + random.nextInt(100), // dim col1 0-2
        "dim_" + random.nextInt(100), // dim col2 0-2
        random.nextInt(1000), // ts
      )
    }
    val df = events.toDF("uid", "eid", "dim1", "dim2", "ts")
    println(s"generated")

//    df.orderBy("uid").show(100, false)

    val wf = WindowFunnel(
      lit(100).expr,
      lit(5).expr,
      Column("ts").expr,
      Column("eid").expr,
      lit(null).expr
    ).toAggregateExpression()

    println(s"started")
    (0 until 5).foreach { _ =>
      val agg = df.groupBy("uid").agg(Column(wf))
      val started = System.currentTimeMillis()
      val result = agg.collect()
      assert(result != null)
      println(s">>>>> collected ${System.currentTimeMillis() - started}ms")
    }
  }


}

object TypedImperativeAggregateSuite {

  /**
   * Calculate the max value with object aggregation buffer. This stores class MaxValue
   * in aggregation buffer.
   */
  private case class TypedMax(
      child: Expression,
      nullable: Boolean = false,
      mutableAggBufferOffset: Int = 0,
      inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[MaxValue] with ImplicitCastInputTypes {


    override def createAggregationBuffer(): MaxValue = {
      // Returns Int.MinValue if all inputs are null
      new MaxValue(Int.MinValue)
    }

    override def update(buffer: MaxValue, input: InternalRow): MaxValue = {
      child.eval(input) match {
        case inputValue: Int =>
          if (inputValue > buffer.value) {
            buffer.value = inputValue
            buffer.isValueSet = true
          }
        case null => // skip
      }
      buffer
    }

    override def merge(bufferMax: MaxValue, inputMax: MaxValue): MaxValue = {
      if (inputMax.value > bufferMax.value) {
        bufferMax.value = inputMax.value
        bufferMax.isValueSet = bufferMax.isValueSet || inputMax.isValueSet
      }
      bufferMax
    }

    override def eval(bufferMax: MaxValue): Any = {
      if (nullable && bufferMax.isValueSet == false) {
        null
      } else {
        bufferMax.value
      }
    }

    override lazy val deterministic: Boolean = true

    override def children: Seq[Expression] = Seq(child)

    override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)

    override def dataType: DataType = IntegerType

    override def withNewMutableAggBufferOffset(newOffset: Int): TypedImperativeAggregate[MaxValue] =
      copy(mutableAggBufferOffset = newOffset)

    override def withNewInputAggBufferOffset(newOffset: Int): TypedImperativeAggregate[MaxValue] =
      copy(inputAggBufferOffset = newOffset)

    override def serialize(buffer: MaxValue): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val stream = new DataOutputStream(out)
      stream.writeBoolean(buffer.isValueSet)
      stream.writeInt(buffer.value)
      out.toByteArray
    }

    override def deserialize(storageFormat: Array[Byte]): MaxValue = {
      val in = new ByteArrayInputStream(storageFormat)
      val stream = new DataInputStream(in)
      val isValueSet = stream.readBoolean()
      val value = stream.readInt()
      new MaxValue(value, isValueSet)
    }
  }

  private class MaxValue(var value: Int, var isValueSet: Boolean = false)
}
