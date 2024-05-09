package com.qubole.sparklens.app

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.EventHistoryReplayer

import java.io.ByteArrayOutputStream

class SparklensApp {

  def analyzeFromEventHistory(sparkConf: SparkConf, appId: String): String = {
    analyzeFromEventHistory(sparkConf, appId, None)
  }

  def analyzeFromEventHistory(sparkConf: SparkConf, appId: String, attemptId: Option[String]): String = {

    val outCapture = new ByteArrayOutputStream()
    val errCapture = new ByteArrayOutputStream()

    Console.withOut(outCapture) {
      Console.withErr(errCapture) {
        new EventHistoryReplayer(sparkConf, appId, attemptId)
      }
    }

    if (outCapture.toString.nonEmpty) {
      // "yes"
      outCapture.toString
    } else if (errCapture.toString.nonEmpty) {
      // "NO!"
      errCapture.toString
    } else {
      throw new IllegalArgumentException(
        s"Failed to analyze event history for appId: $appId and attemptId: $attemptId")
    }
  }
}
