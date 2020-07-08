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
// scalastyle:off
package org.apache.spark.scheduler

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Locale

import org.apache.commons.compress.utils.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}
import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A SparkListener that logs events to persistent storage.
 *
 * Event logging is specified by the following configurable parameters:
 *   spark.eventLog.enabled - Whether event logging is enabled.
 *   spark.eventLog.logBlockUpdates.enabled - Whether to log block updates
 *   spark.eventLog.compress - Whether to compress logged events
 *   spark.eventLog.overwrite - Whether to overwrite any existing files.
 *   spark.eventLog.dir - Path to the directory in which events are logged.
 *   spark.eventLog.buffer.kb - Buffer size to use when writing to output streams
 */
private[spark] class EventLoggingListener(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SparkListener with Logging {

  import EventLoggingListener._

  def this(appId: String, appAttemptId : Option[String], logBaseDir: URI, sparkConf: SparkConf) =
    this(appId, appAttemptId, logBaseDir, sparkConf,
      SparkHadoopUtil.get.newConfiguration(sparkConf))

  private val shouldCompress = sparkConf.get(EVENT_LOG_COMPRESS)
  private val shouldOverwrite = sparkConf.get(EVENT_LOG_OVERWRITE)
  private val shouldLogBlockUpdates = sparkConf.get(EVENT_LOG_BLOCK_UPDATES)
  private val testing = sparkConf.get(EVENT_LOG_TESTING)
  private val outputBufferSize = sparkConf.get(EVENT_LOG_OUTPUT_BUFFER_SIZE).toInt
  private val fileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)

  private val ROLL_LOG_DIR_NAME_PREFIX = "eventlog_v2_"
  private val ROLL_LOG_FILE_NAME_PREFIX = "events_"
  private var rollIndex: Long = 0L
  private val eventRollFileMaxLength = sparkConf.get(EVENT_LOG_ROLLING_MAX_FILE_SIZE)

  private val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf))
    } else {
      None
    }
  private val compressionCodecName = compressionCodec.map { c =>
    CompressionCodec.getShortName(c.getClass.getName)
  }

  // Only defined if the file system scheme is not local
  private var hadoopDataStream: Option[FSDataOutputStream] = None

  private var writer: Option[PrintWriter] = None

  // For testing. Keep track of all JSON serialized events that have been logged.
  private[scheduler] val loggedEvents = new ArrayBuffer[JValue]

  // Visible for tests only.
  private[scheduler] var logPath = getLogPath(logBaseDir, appId, appAttemptId, compressionCodecName)

  // For roll event log
  private[scheduler] var rollDirPath = new Path(new Path(logBaseDir), nameForEventLogDir(appId, appAttemptId))
  private var currentRollLogFilePath: Path = _
  private var countingRollOutputStream: Option[CountingOutputStream] = None


  /**
   * Creates the log file in the configured log directory.
   */
  def start() {
    if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDirectory) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir is not a directory.")
    }

    if (!sparkConf.get(EVENT_LOG_ENABLE_ROLLING)) {
      val path = new Path(logPath + IN_PROGRESS)
      initLogFile(path){ os => new PrintWriter(os) }
    } else {
      if (fileSystem.exists(rollDirPath) && shouldOverwrite) {
        fileSystem.delete(rollDirPath, true)
      }
      fileSystem.mkdirs(rollDirPath, LOG_FILE_PERMISSIONS)
      rollEventLogFile(null)
    }

  }


  protected def initLogFile(path: Path)(fnSetupWriter: OutputStream => PrintWriter): Unit = {
    if (shouldOverwrite && fileSystem.delete(path, true)) {
      logWarning(s"Event log $path already exists. Overwriting...")
    }

    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"
    val uri = path.toUri

    // The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
    // Therefore, for local files, use FileOutputStream instead.
    val dstream =
    if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
      new FileOutputStream(uri.getPath)
    } else {
      hadoopDataStream = Some(
        SparkHadoopUtil.createFile(fileSystem, path, sparkConf.get(EVENT_LOG_ALLOW_EC)))
      hadoopDataStream.get
    }

    try {
      val cstream = compressionCodec.map(_.compressedOutputStream(dstream))
        .getOrElse(dstream)
      val bstream = new BufferedOutputStream(cstream, outputBufferSize)
      fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)
      EventLoggingListener.initEventLog(bstream, testing, loggedEvents)
      logInfo(s"Logging events to $path")
      writer = Some(fnSetupWriter(bstream))
    } catch {
      case e: Exception =>
        dstream.close()
        throw e
    }
  }

  def rollEventLogFile(logRollUp: SparkListenerLogRollUp): Unit ={
    closeWriter()
    if (logRollUp != null && logRollUp.checkTime != null) {
      SparkHadoopUtil.createFile(fileSystem, new Path(new Path(logBaseDir), logRollUp.checkTime), sparkConf.get(EVENT_LOG_ALLOW_EC)).close()
    }
    rollIndex += 1
    val now = System.currentTimeMillis()
    val base = s"${ROLL_LOG_FILE_NAME_PREFIX}${rollIndex}_" + nameForAppAndAttempt(appId, appAttemptId) +
      "_" + now
    if( currentRollLogFilePath != null){
      fileSystem.rename(currentRollLogFilePath,new Path(currentRollLogFilePath.toUri.getPath + "_" + now))
    }
    currentRollLogFilePath = new Path(rollDirPath, base)

    initLogFile(currentRollLogFilePath){ os =>
      countingRollOutputStream = Some(new CountingOutputStream(os))
      new PrintWriter(
        new OutputStreamWriter(countingRollOutputStream.get, StandardCharsets.UTF_8))
    }
  }

  def nameForEventLogDir(appId: String, appAttemptId: Option[String]): String = {
    ROLL_LOG_DIR_NAME_PREFIX + nameForAppAndAttempt(appId, appAttemptId) + "#" + System.currentTimeMillis()
  }

  def nameForAppAndAttempt(appId: String, appAttemptId: Option[String]): String = {
    val base = sanitizeDirName(appId)
    if (appAttemptId.isDefined) {
      base + "_" + sanitizeDirName(appAttemptId.get)
    } else {
      base
    }
  }

  def sanitizeDirName(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase(Locale.ROOT)
  }

  protected def closeWriter(): Unit = {
    writer.foreach(_.close())
  }

  /** Log the event as JSON. */
  private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false) {

    if(sparkConf.get(EVENT_LOG_ENABLE_ROLLING)){
      writer.foreach { w =>
        val currentLen = countingRollOutputStream.get.getBytesWritten
        if (currentLen  > eventRollFileMaxLength) {
          rollEventLogFile(null)
        }
      }
    }

    val eventJson = JsonProtocol.sparkEventToJson(event)
    // scalastyle:off println
    writer.foreach(_.println(compact(render(eventJson))))
    // scalastyle:on println
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(_.hflush())
    }
    if (testing) {
      loggedEvents += eventJson
    }
  }

  // Events that do not trigger a flush
  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = logEvent(event)

  override def onTaskStart(event: SparkListenerTaskStart): Unit = logEvent(event)

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = logEvent(event)

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = logEvent(event)

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    logEvent(redactEvent(event))
  }

  // Events that trigger a flush
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = logEvent(event, flushLogger = true)

  override def onJobEnd(event: SparkListenerJobEnd): Unit = logEvent(event, flushLogger = true)

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    logEvent(event, flushLogger = true)
  }
  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorBlacklistedForStage(
      event: SparkListenerExecutorBlacklistedForStage): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeBlacklistedForStage(event: SparkListenerNodeBlacklistedForStage): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    if (shouldLogBlockUpdates) {
      logEvent(event, flushLogger = true)
    }
  }

  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = { }


  override def onSparkListenerLogRollUp(logRollUp: SparkListenerLogRollUp): Unit = {
    rollEventLogFile(logRollUp)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (event.logEvent) {
      logEvent(event, flushLogger = true)
    }
  }

  /**
   * Stop logging events. The event log file will be renamed so that it loses the
   * ".inprogress" suffix.
   */
  def stop(): Unit = {
    writer.foreach(_.close())

    if(!sparkConf.get(EVENT_LOG_ENABLE_ROLLING)){
      val target = new Path(logPath)
      if (fileSystem.exists(target)) {
        if (shouldOverwrite) {
          logWarning(s"Event log $target already exists. Overwriting...")
          if (!fileSystem.delete(target, true)) {
            logWarning(s"Error deleting $target")
          }
        } else {
          throw new IOException("Target log file already exists (%s)".format(logPath))
        }
      }
      fileSystem.rename(new Path(logPath + IN_PROGRESS), target)
      // touch file to ensure modtime is current across those filesystems where rename()
      // does not set it, -and which support setTimes(); it's a no-op on most object stores
      try {
        fileSystem.setTimes(target, System.currentTimeMillis(), -1)
      } catch {
        case e: Exception => logDebug(s"failed to set time of $target", e)
      }
    }else {
      if( currentRollLogFilePath != null){
        fileSystem.rename(currentRollLogFilePath,new Path(currentRollLogFilePath.toUri.getPath + "_" + System.currentTimeMillis()))
        currentRollLogFilePath = null
      }
    }
  }

  private[spark] def redactEvent(
      event: SparkListenerEnvironmentUpdate): SparkListenerEnvironmentUpdate = {
    // environmentDetails maps a string descriptor to a set of properties
    // Similar to:
    // "JVM Information" -> jvmInformation,
    // "Spark Properties" -> sparkProperties,
    // ...
    // where jvmInformation, sparkProperties, etc. are sequence of tuples.
    // We go through the various  of properties and redact sensitive information from them.
    val redactedProps = event.environmentDetails.map{ case (name, props) =>
      name -> Utils.redact(sparkConf, props)
    }
    SparkListenerEnvironmentUpdate(redactedProps)
  }

}

private[spark] object EventLoggingListener extends Logging {
  // Suffix applied to the names of files still being written by applications.
  val IN_PROGRESS = ".inprogress"
  val DEFAULT_LOG_DIR = "/tmp/spark-events"

  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  // A cache for compression codecs to avoid creating the same codec many times
  private val codecMap = new mutable.HashMap[String, CompressionCodec]

  /**
   * Write metadata about an event log to the given stream.
   * The metadata is encoded in the first line of the event log as JSON.
   *
   * @param logStream Raw output stream to the event log file.
   */
  def initEventLog(
      logStream: OutputStream,
      testing: Boolean,
      loggedEvents: ArrayBuffer[JValue]): Unit = {
    val metadata = SparkListenerLogStart(SPARK_VERSION)
    val eventJson = JsonProtocol.logStartToJson(metadata)
    val metadataJson = compact(eventJson) + "\n"
    logStream.write(metadataJson.getBytes(StandardCharsets.UTF_8))
    if (testing && loggedEvents != null) {
      loggedEvents += eventJson
    }
  }

  /**
   * Return a file-system-safe path to the log file for the given application.
   *
   * Note that because we currently only create a single log file for each application,
   * we must encode all the information needed to parse this event log in the file name
   * instead of within the file itself. Otherwise, if the file is compressed, for instance,
   * we won't know which codec to use to decompress the metadata needed to open the file in
   * the first place.
   *
   * The log file name will identify the compression codec used for the contents, if any.
   * For example, app_123 for an uncompressed log, app_123.lzf for an LZF-compressed log.
   *
   * @param logBaseDir Directory where the log file will be written.
   * @param appId A unique app ID.
   * @param appAttemptId A unique attempt id of appId. May be the empty string.
   * @param compressionCodecName Name to identify the codec used to compress the contents
   *                             of the log, or None if compression is not enabled.
   * @return A path which consists of file-system-safe characters.
   */
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String] = None): String = {
    val base = new Path(logBaseDir).toString.stripSuffix("/") + "/" + sanitize(appId)
    val codec = compressionCodecName.map("." + _).getOrElse("")
    if (appAttemptId.isDefined) {
      base + "_" + sanitize(appAttemptId.get) + codec
    } else {
      base + codec
    }
  }

  private def sanitize(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase(Locale.ROOT)
  }

  /**
   * Opens an event log file and returns an input stream that contains the event data.
   *
   * @return input stream that holds one JSON record per line.
   */
  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    val in = new BufferedInputStream(fs.open(log))
    try {
      val codec = codecName(log).map { c =>
        codecMap.getOrElseUpdate(c, CompressionCodec.createCodec(new SparkConf, c))
      }
      codec.map(_.compressedInputStream(in)).getOrElse(in)
    } catch {
      case e: Throwable =>
        in.close()
        throw e
    }
  }

  def codecName(log: Path): Option[String] = {
    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(IN_PROGRESS)
    logName.split("\\.").tail.lastOption
  }

}
