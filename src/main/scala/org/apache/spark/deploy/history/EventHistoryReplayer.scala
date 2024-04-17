package org.apache.spark.deploy.history

import java.net.URI
import java.util.Locale

import com.qubole.sparklens.QuboleJobListener
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{EVENT_LOG_DIR, History}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils.tryWithResource

import java.io.FileNotFoundException
import scala.util.{Failure, Success, Try}


class EventHistoryReplayer(sparkConf: SparkConf, appId: String, attemptId: Option[String]) extends InternalEventHistoryReplayer(sparkConf
  .set("spark.sparklens.reporting.disabled", "false")
  .set("spark.sparklens.save.data", "false")
  .set(History.HISTORY_LOG_DIR, sparkConf.get(EVENT_LOG_DIR)),
  appId,
  attemptId) {

}

class InternalEventHistoryReplayer(sparkConf: SparkConf, appId: String, attemptId: Option[String]) extends FsHistoryProvider(sparkConf) {

  private def sanitize(str: String): String =
    str.replaceAll("[ :/]", "-")
        .replaceAll("[.${}'\"]", "_")
        .toLowerCase(Locale.ROOT)

  // This is private
  val logDir = sparkConf.get(History.HISTORY_LOG_DIR)

  val fsHistoryProvider = new FsHistoryProvider(sparkConf)

  val replayListenerBus = new ReplayListenerBus()
  replayListenerBus.addListener(new QuboleJobListener(sparkConf))

  def allCodecOptions: Seq[Option[String]] = {
    // Because CompressionCodec has codec list as private
    val codecs: Seq[Option[String]] = List(
      None, Some("lz4"), Some("lzf"), Some("snappy"), Some("zstd"))
    try {
      val preferredCode: String = CompressionCodec.getCodecName(sparkConf)
      List(Some(preferredCode)) ++ codecs
    } catch {
      case e: Exception =>
        codecs
    }
  }

  private def getLogPath(
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

  def getLogPaths(appId: String, attemptId: Option[String]) = {
    val logBaseDir = Utils.resolveURI(sparkConf.get(EVENT_LOG_DIR))
    val rollingPath = RollingEventLogFilesWriter.getAppEventLogDirPath(
      logBaseDir, appId, attemptId).toString()
    val singlePath = SingleEventLogFileWriter.getLogPath(
      logBaseDir, appId, attemptId)
    val paths: Seq[String] = allCodecOptions.flatMap { codec =>
      val logPath = getLogPath(Utils.resolveURI(logDir), appId, attemptId, codec)
      val inProgressPath = logPath + EventLogFileWriter.IN_PROGRESS
      Seq(logPath, inProgressPath)
    } ++ Seq(rollingPath)
    logDebug(s"Searching paths ${paths} for ${appId}")
    paths
  }

  getLogPaths(appId, attemptId)
    .map(new Path(_))
    .flatMap(tryGetFileStatus)
    .headOption // find the first valid log path amongst all possible log paths
    .flatMap(EventLogFileReader(fsHistoryProvider.fs, _))
    .foreach { reader => {
      // stop replaying next log files if ReplayListenerBus indicates some error or halt
      var continueReplay = true
      reader.listEventLogFiles.foreach { file =>
        if (continueReplay) {
          tryWithResource(EventLogFileReader.openEventLog(file.getPath, fsHistoryProvider.fs)) { in =>
            continueReplay = replayListenerBus.replay(in, file.getPath.toString)
          }
        }
      }
    }
  }

  private def tryGetFileStatus(path: Path): Option[FileStatus] =
    Try(fsHistoryProvider.fs.getFileStatus(path)) match {
      case Success(status) => Some(status)
      case Failure(_: FileNotFoundException) => None
      case Failure(e) => throw e
    }
}
