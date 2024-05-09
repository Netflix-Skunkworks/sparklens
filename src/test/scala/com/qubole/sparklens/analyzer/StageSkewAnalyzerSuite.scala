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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

import scala.collection.mutable

class StageSkewAnalyzerSuite extends AnyFunSuite {

  val startTime = 0
  val endTime = 60000000000L
  // Make a non-SQL task
  val stage1 = new StageTimeSpan(1, 1, false)
  // SQL tasks
  // stage 2 should impact the target number of partitions
  val stage2 = new StageTimeSpan(2, 2, true)
  // stage 3 should have us turn off AQE if present
  val stage3 = new StageTimeSpan(3, 1, true)
  stage1.shuffleRead = true
  stage1.taskExecutionTimes = Array[Int](6000000)
  stage2.shuffleRead = true
  stage2.taskExecutionTimes = 0.to(200).map(x => 60000 * 2).toArray
  stage3.shuffleRead = true
  // One really long task
  stage3.taskExecutionTimes = Array[Int](60000000)

  def createDummyAppContext(stageTimeSpans: mutable.HashMap[Int, StageTimeSpan]): AppContext = {

    val jobMap = new mutable.HashMap[Long, JobTimeSpan]

    val jobSQLExecIDMap = new mutable.HashMap[Long, Long]

    val execStartTimes = new mutable.HashMap[String, ExecutorTimeSpan]()

    val appInfo = new ApplicationInfo()
    appInfo.startTime = startTime
    appInfo.endTime = endTime


    val conf = new SparkConf()

    new AppContext(appInfo,
      new AggregateMetrics(),
      mutable.HashMap[String, HostTimeSpan](),
      mutable.HashMap[String, ExecutorTimeSpan](),
      jobMap,
      jobSQLExecIDMap,
      stageTimeSpans,
      mutable.HashMap[Int, Long](),
      Some(conf.getAll.toMap))
  }

  test("Change number of partitions when not skewed but long") {
    val stageTimeSpans = new mutable.HashMap[Int, StageTimeSpan]
    stageTimeSpans(1) = stage1
    stageTimeSpans(2) = stage2
    val ac = createDummyAppContext(stageTimeSpans)
    val sska = new StageSkewAnalyzer()
    val suggestions = sska.computeSuggestions(ac)
    assert(suggestions.get("spark.sql.adaptive.coalescePartitions.enabled") == None,
      "Leave AQE on if we 'just' have long partitions")
    assert(suggestions.get("spark.sql.shuffle.partitions") == Some("400"),
      "We aim for lots of partitions")
  }

  test("Turn off AQE if stage 3 is present") {
    val stageTimeSpans = new mutable.HashMap[Int, StageTimeSpan]
    stageTimeSpans(1) = stage1
    stageTimeSpans(3) = stage3
    val ac = createDummyAppContext(stageTimeSpans)
    val sska = new StageSkewAnalyzer()
    val suggestions = sska.computeSuggestions(ac)
    assert(suggestions.get("spark.sql.adaptive.coalescePartitions.enabled") == Some("false"),
      "Turn of AQE when bad coalesce occurs")
  }

  test("stage 1 should do nothing") {
    val stageTimeSpans = new mutable.HashMap[Int, StageTimeSpan]
    stageTimeSpans(1) = stage1
    val ac = createDummyAppContext(stageTimeSpans)
    val sska = new StageSkewAnalyzer()
    val suggestions = sska.computeSuggestions(ac)
    assert(suggestions == Map.empty[String, String], "Don't suggest on non-SQL stages")
  }
}
