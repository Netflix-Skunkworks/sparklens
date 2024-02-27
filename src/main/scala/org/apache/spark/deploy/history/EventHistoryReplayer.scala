package org.apache.spark.deploy.history

import com.qubole.sparklens.QuboleJobListener
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{EVENT_LOG_DIR, History}
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.util.Utils.tryWithResource

import java.io.FileNotFoundException
import scala.util.{Failure, Success, Try}

class EventHistoryReplayer(sparkConf: SparkConf, appId: String, attemptId: Option[String]) {

  sparkConf
    .set("spark.sparklens.reporting.disabled", "false")
    .set("spark.sparklens.save.data", "false")
    .set(History.HISTORY_LOG_DIR, sparkConf.get(EVENT_LOG_DIR))

  val fsHistoryProvider = new FsHistoryProvider(sparkConf)

  val replayListenerBus = new ReplayListenerBus()
  replayListenerBus.addListener(new QuboleJobListener(sparkConf))

  fsHistoryProvider.getLogPaths(appId, attemptId)
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
