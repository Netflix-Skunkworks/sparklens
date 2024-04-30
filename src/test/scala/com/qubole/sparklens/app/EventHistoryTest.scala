package com.qubole.sparklens.app

import java.io.{ByteArrayOutputStream, PrintStream}

import com.qubole.sparklens.TestUtils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

class EventHistoryFileReportingSuite extends AnyFunSuite with Matchers {

  test("Reporting from our static sparklens should be reasonable") {
    val eventHistoryFile = s"${System.getProperty("user.dir")}" +
      s"/src/test/event-history-test-files/local-1532512550423"

    validateOutput(outputFromEventHistoryReport(eventHistoryFile),
      appId = "local-1532512550423",
      updatedConf = Map.empty[String, String])
  }

  test("Reporting from our fresh sparklens should be reasonable") {
    val eventHistoryFile = s"${System.getProperty("user.dir")}" +
      s"/src/test/event-history-test-files/local-fresh"

    validateOutput(outputFromEventHistoryReport(eventHistoryFile),
      appId = "loal-",
      updatedConf = Map.empty[String, String])
  }

  test("Reporting from our fresh sparklens should be reasonable") {
    val eventHistoryFile = s"${System.getProperty("user.dir")}" +
      s"/src/test/event-history-test-files/local-fresh-dynamic"

    validateOutput(outputFromEventHistoryReport(eventHistoryFile),
      appId = "kube-",
      updatedConf = Map.empty[String, String],
      dynamic=true)
  }

  private def outputFromEventHistoryReport(file: String): String = {
    val out = new ByteArrayOutputStream()
    Console.withOut(new PrintStream(out)) {
      new EventHistoryReporter(file)
    }
    out.toString
  }

  private def validateOutput(
    fileContents: String,
    appId: String,
    updatedConf: Map[String, String],
    dynamic: Boolean = false) = {
    fileContents should include(s"ID $appId")
    if (dynamic) {
      fileContents should include("i don't remember")
    } else {
      fileContents should include("since not dynamic")
    }
  }
}
