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

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.core.ProblemFilters._

/**
 * Additional excludes for checking of Spark's binary compatibility.
 *
 * This acts as an official audit of cases where we excluded other classes. Please use the narrowest
 * possible exclude here. MIMA will usually tell you what exclude to use, e.g.:
 *
 * ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.take")
 *
 * It is also possible to exclude Spark classes and packages. This should be used sparingly:
 *
 * MimaBuild.excludeSparkClass("graphx.util.collection.GraphXPrimitiveKeyOpenHashMap")
 *
 * For a new Spark version, please update MimaBuild.scala to reflect the previous version.
 */
object MimaExcludes {

  // Exclude rules for 3.5.x from 3.4.0
  lazy val v35excludes = defaultExcludes ++ Seq(
    // [SPARK-44531][CONNECT][SQL] Move encoder inference to sql/api
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DataTypes"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.SQLUserDefinedType"),
    // [SPARK-43165][SQL] Move canWrite to DataTypeUtils
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.types.DataType.canWrite"),
    // [SPARK-43792][SQL][PYTHON][CONNECT] Add optional pattern for Catalog.listCatalogs
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.listCatalogs"),
    // [SPARK-43881][SQL][PYTHON][CONNECT] Add optional pattern for Catalog.listDatabases
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.listDatabases"),
    // [SPARK-43961][SQL][PYTHON][CONNECT] Add optional pattern for Catalog.listTables
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.listTables"),
    // [SPARK-43992][SQL][PYTHON][CONNECT] Add optional pattern for Catalog.listFunctions
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.listFunctions"),
    // [SPARK-43952][CORE][CONNECT][SQL] Add SparkContext APIs for query cancellation by tag
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.JobData.this"),
    // [SPARK-44205][SQL] Extract Catalyst Code from DecimalType
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.types.DecimalType.unapply"),
    // [SPARK-44507][SQL][CONNECT] Move AnalysisException to sql/api.
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.AnalysisException"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.AnalysisException$"),
    // [SPARK-44686][CONNECT][SQL] Add the ability to create a RowEncoder in Encoders
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.RowFactory"),
    // [SPARK-44535][CONNECT][SQL] Move required Streaming API to sql/api
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.GroupStateTimeout"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.OutputMode"),
    // [SPARK-44692][CONNECT][SQL] Move Trigger(s) to sql/api
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.Trigger"),
    // [SPARK-44713][CONNECT][SQL] Move shared classes to sql/api
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.MapGroupsWithStateFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SaveMode"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.GroupState"),
    // [SPARK-46480][CORE][SQL] Fix NPE when table cache task attempt
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.isFailed"),

    // SPARK-43299: Convert StreamingQueryException in Scala Client
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.StreamingQueryException"),

    // SPARK-45856: Move ArtifactManager from Spark Connect into SparkSession (sql/core)
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.apply"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.userId"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.sessionId"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.copy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.copy$default$3"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.this"),
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.storage.CacheId$"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.apply"),

    // SPARK-46410: Assign error classes/subclasses to JdbcUtils.classifyException
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.JdbcDialect.classifyException"),
    // TODO(SPARK-46878): Invalid Mima report for StringType extension
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.types.StringType.this"),
    // SPARK-47011: Remove deprecated BinaryClassificationMetrics.scoreLabelsWeight
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.scoreLabelsWeight"),
    // SPARK-46938: Javax -> Jakarta namespace change.
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ui.ProxyRedirectHandler$ResponseWrapper"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ui.ProxyRedirectHandler#ResponseWrapper.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.DB2Dialect#DB2SQLBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.DB2Dialect#DB2SQLQueryBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.MsSqlServerDialect#MsSqlServerSQLBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.MsSqlServerDialect#MsSqlServerSQLQueryBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.MySQLDialect#MySQLSQLBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.MySQLDialect#MySQLSQLQueryBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.OracleDialect#OracleSQLBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.OracleDialect#OracleSQLQueryBuilder.this"),
    // SPARK-47706: Bump json4s from 3.7.0-M11 to 4.0.7
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.expressions.MutableAggregationBuffer.jsonValue"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.types.DataType#JSortedObject.unapplySeq"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.mllib.tree.model.TreeEnsembleModel#SaveLoadV1_0.readMetadata"),
    // SPARK-47814: Move `KinesisTestUtils` & `WriteInputFormatTestDataGenerator` from `main` to `test`
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.python.TestWritable"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.python.TestWritable$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.python.WriteInputFormatTestDataGenerator"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.python.WriteInputFormatTestDataGenerator$"),
    // SPARK-47764: Cleanup shuffle dependencies based on ShuffleCleanupMode
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.shuffle.MigratableResolver.addShuffleToSkip"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLContext#implicits._sqlContext"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLImplicits._sqlContext"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.SQLImplicits.session"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SparkSession#implicits._sqlContext"),
    // SPARK-48761: Add clusterBy() to CreateTableWriter.
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.CreateTableWriter.clusterBy"),
    // SPARK-48901: Add clusterBy() to DataStreamWriter.
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.DataStreamWriter.clusterBy")
  )

  // Default exclude rules
  lazy val defaultExcludes = Seq(
    // Spark Internals
    ProblemFilters.exclude[Problem]("org.apache.spark.rpc.*"),
    ProblemFilters.exclude[Problem]("org.spark-project.jetty.*"),
    ProblemFilters.exclude[Problem]("org.spark_project.jetty.*"),
    ProblemFilters.exclude[Problem]("org.sparkproject.jetty.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.internal.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.unused.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.unsafe.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.memory.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.util.collection.unsafe.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.catalyst.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.execution.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.internal.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.errors.*"),
    // DSv2 catalog and expression APIs are unstable yet. We should enable this back.
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.connector.catalog.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.connector.expressions.*"),
    // Avro source implementation is internal.
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.v2.avro.*"),

    // SPARK-43169: shaded and generated protobuf code
    ProblemFilters.exclude[Problem]("org.sparkproject.spark_core.protobuf.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.status.protobuf.StoreTypes*"),

    // SPARK-43265: Move Error framework to a common utils module
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.QueryContext"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.SparkException"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.SparkException$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.SparkThrowable"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ErrorInfo$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ErrorSubInfo$"),

    // SPARK-44104: shaded protobuf code and Apis with parameters relocated
    ProblemFilters.exclude[Problem]("org.sparkproject.spark_protobuf.protobuf.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.protobuf.utils.SchemaConverters.*"),

    // SPARK-44255: Relocate StorageLevel to common/utils
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.storage.StorageLevel"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.storage.StorageLevel$"),

    // SPARK-44475: Relocate DataType and Parser to sql/api
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ArrayType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ArrayType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.BinaryType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.BinaryType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.BooleanType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.BooleanType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ByteType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ByteType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.CalendarIntervalType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.CalendarIntervalType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.CharType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.CharType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DataType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DataType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DateType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DateType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DayTimeIntervalType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DayTimeIntervalType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.Decimal"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.Decimal$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ShortType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ShortType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StringType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StringType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StructField"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StructField$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StructType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StructType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.TimestampNTZType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.TimestampNTZType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.TimestampType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.TimestampType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.UDTRegistration"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.UDTRegistration$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.VarcharType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.VarcharType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.YearMonthIntervalType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.YearMonthIntervalType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DecimalType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DecimalType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType$DoubleAsIfIntegral"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType$DoubleAsIfIntegral$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType$DoubleIsConflicted"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType$FloatAsIfIntegral"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType$FloatAsIfIntegral$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType$FloatIsConflicted"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.IntegerType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.IntegerType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.LongType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.LongType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.MapType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.MapType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.Metadata"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.Metadata$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.MetadataBuilder"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.NullType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.NullType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ObjectType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ObjectType$"),

    // SPARK-44496: Move Interfaces needed by SCSC to sql/api.
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.Encoder"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.Row"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.Row$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.package"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.package$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.CoGroupFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.DoubleFlatMapFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.DoubleFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.FilterFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.FlatMapFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.FlatMapFunction2"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.FlatMapGroupsFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.ForeachFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.ForeachPartitionFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.Function"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.Function0"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.Function2"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.Function3"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.Function4"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.MapFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.MapGroupsFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.MapPartitionsFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.PairFlatMapFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.PairFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.ReduceFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.VoidFunction"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.java.function.VoidFunction2"),

    // SPARK-43997: UDF* classes needed by SCSC and moved to sql/api
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.api.java.UDF*"),

    (problem: Problem) => problem match {
      case MissingClassProblem(cls) => !cls.fullName.startsWith("org.sparkproject.jpmml") &&
          !cls.fullName.startsWith("org.sparkproject.dmg.pmml")
      case _ => true
    }
  )

  def excludes(version: String) = version match {
    case v if v.startsWith("3.5") => v35excludes
    case _ => Seq()
  }
}
