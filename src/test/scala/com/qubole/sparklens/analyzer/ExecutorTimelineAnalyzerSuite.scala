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

package com.qubole.sparklens.analyzer

import com.qubole.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import com.qubole.sparklens.helper.JobOverlapHelper

import org.scalatest.FunSuite

import org.apache.spark.SparkConf

import scala.collection.mutable

class ExecutorTimelineAnalyzerSuite extends FunSuite {

  val startTime = 0
  val endTime = 60000000000L
  def createDummyAppContext(): AppContext = {

    val jobMap = new mutable.HashMap[Long, JobTimeSpan]

    val jobSQLExecIDMap = new mutable.HashMap[Long, Long]

    val execStartTimes = new mutable.HashMap[String, ExecutorTimeSpan]()

    val appInfo = new ApplicationInfo()
    appInfo.startTime = startTime
    appInfo.endTime = endTime

    val firstExec = new ExecutorTimeSpan("1", "1", 1)
    firstExec.setStartTime(0)
    firstExec.setEndTime(6000000)
    execStartTimes("1") = firstExec
    // We start an 2nd exec but exit "right away".
    val secondExec = new ExecutorTimeSpan("2", "1", 1)
    secondExec.setStartTime(0)
    secondExec.setEndTime(120)
    execStartTimes("2") = secondExec


    val conf = new SparkConf().set("spark.dynamicAllocation.enabled", "true")

    new AppContext(appInfo,
      new AggregateMetrics(),
      mutable.HashMap[String, HostTimeSpan](),
      execStartTimes,
      jobMap,
      jobSQLExecIDMap,
      mutable.HashMap[Int, StageTimeSpan](),
      mutable.HashMap[Int, Long](),
      conf.getAll.toMap)
  }

  test("A reasonable guess at initial executors") {
    val ac = createDummyAppContext()
    val eta = new ExecutorTimelineAnalyzer()
    val (logs, suggestions) = eta.analyzeAndSuggest(ac, startTime, endTime)
    assert(suggestions.get("spark.dynamicAllocation.initialExecutors") == Some("1"),
      "Initial exec suggestions")
  }
}
