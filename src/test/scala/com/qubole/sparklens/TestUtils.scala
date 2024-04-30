package com.qubole.sparklens

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.TimeZone
import scala.io.Source

object TestUtils {
  def getFileContents(fileName: String): String = {
    val bufferedSource = Source.fromFile(fileName)
    val result = bufferedSource.mkString
    bufferedSource.close
    result
  }

}

trait TimezoneSuite extends AnyFunSuite with BeforeAndAfterAll {

  val defaultTimeZone: TimeZone = TimeZone.getDefault
  override def beforeAll(): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override def afterAll(): Unit = {
    TimeZone.setDefault(defaultTimeZone)
  }
}
