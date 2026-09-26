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

package org.apache.spark.status

import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.apache.spark.{InternalAccumulator, SparkConf, SparkContext, Success, TaskState}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config.Status._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._
import org.apache.spark.status.ListenerEventsTestHelper.createExecutorAddedEvent
import org.apache.spark.util.kvstore.InMemoryStore

/**
 * Benchmark for live status updates.
 *
 * Compares a heartbeat flush and a stage completion when live entities are unchanged with the
 * same calls when every entity is dirty. The dirty cases are what the listener used to do on
 * every heartbeat and stage completion: rebuild and reindex an immutable snapshot of each entity.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain org.apache.spark.status.AppStatusListenerBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *        "core/Test/runMain org.apache.spark.status.AppStatusListenerBenchmark"
 *      Results will be written to "benchmarks/AppStatusListenerBenchmark-results.txt".
 * }}}
 */
object AppStatusListenerBenchmark extends BenchmarkBase {

  private val numExecutors = 200
  private val tasksPerExecutor = 10
  private val numTasks = numExecutors * tasksPerExecutor

  // Stage completion also updates the stage, job, and pool. Use enough executor summaries
  // that rewriting them, rather than those three records, is what the timer measures.
  private val stageExecutors = 4000
  private val stageTasksPerExecutor = 1

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Live status store writes") {
      flushBenchmark()
      stageCompletionBenchmark()
    }
  }

  private def flushBenchmark(): Unit = {
    val benchmark = new Benchmark(
      s"Heartbeat flush ($numTasks tasks, $numExecutors executors)",
      numTasks,
      warmupTime = 0.seconds,
      output = output)
    benchmark.addTimerCase("clean entities", numIters = 5) { timer =>
      withFixture(finishTasks = false) { fixture =>
        armFlush(fixture.listener)
        timer.startTiming()
        fixture.listener.onExecutorMetricsUpdate(
          SparkListenerExecutorMetricsUpdate("0", Nil))
        timer.stopTiming()
      }
    }
    benchmark.addTimerCase("every entity dirty", numIters = 5) { timer =>
      withFixture(finishTasks = false) { fixture =>
        markAllDirty(fixture.listener)
        armFlush(fixture.listener)
        timer.startTiming()
        fixture.listener.onExecutorMetricsUpdate(
          SparkListenerExecutorMetricsUpdate("0", Nil))
        timer.stopTiming()
      }
    }
    // Relative time: less is better. The first case is the baseline.
    benchmark.run(relativeTime = true)
  }

  private def stageCompletionBenchmark(): Unit = {
    val benchmark = new Benchmark(
      s"Stage completion ($stageExecutors executor summaries)",
      stageExecutors,
      warmupTime = 0.seconds,
      output = output)
    benchmark.addTimerCase("summaries already flushed", numIters = 5) { timer =>
      withFixture(finishTasks = true, stageExecutors, stageTasksPerExecutor) { fixture =>
        timer.startTiming()
        fixture.listener.onStageCompleted(SparkListenerStageCompleted(fixture.stage))
        timer.stopTiming()
      }
    }
    benchmark.addTimerCase("every summary dirty", numIters = 5) { timer =>
      withFixture(finishTasks = true, stageExecutors, stageTasksPerExecutor) { fixture =>
        markAllDirty(fixture.listener)
        timer.startTiming()
        fixture.listener.onStageCompleted(SparkListenerStageCompleted(fixture.stage))
        timer.stopTiming()
      }
    }
    benchmark.run(relativeTime = true)
  }

  private case class Fixture(
      listener: AppStatusListener,
      store: ElementTrackingStore,
      stage: StageInfo)

  /**
   * One active stage. A metrics heartbeat writes every live entity, leaving it clean.
   * When `finishTasks` is set, each executor's last task flushes its stage summary.
   */
  private def prepare(finishTasks: Boolean, executors: Int, tasksPerExec: Int): Fixture = {
    val taskCount = executors * tasksPerExec
    val conf = new SparkConf()
      .set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
      .set(LIVE_ENTITY_UPDATE_MIN_FLUSH_PERIOD, 0L)
      .set(ASYNC_TRACKING_ENABLED, false)
    val store = new ElementTrackingStore(new InMemoryStore(), conf)
    val listener = new AppStatusListener(store, conf, live = true)
    var executorId = 0
    while (executorId < executors) {
      listener.onExecutorAdded(createExecutorAddedEvent(executorId))
      executorId += 1
    }

    val props = new Properties()
    props.setProperty(SparkContext.SPARK_SCHEDULER_POOL, "pool")
    val stage = new StageInfo(
      1, 0, "stage", taskCount, Nil, Nil, "details",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    listener.onJobStart(SparkListenerJobStart(1, 0L, Seq(stage), props))
    stage.submissionTime = Some(1L)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage, props))

    val tasksByExecutor = Array.fill(executors)(new ArrayBuffer[TaskInfo](tasksPerExec))
    var taskId = 0
    while (taskId < taskCount) {
      val execIndex = taskId % executors
      val exec = execIndex.toString
      val task = new TaskInfo(
        taskId, taskId, 0, taskId, 1L, exec, exec + ".example.com",
        TaskLocality.PROCESS_LOCAL, speculative = false)
      listener.onTaskStart(SparkListenerTaskStart(stage.stageId, stage.attemptNumber(), task))
      tasksByExecutor(execIndex) += task
      taskId += 1
    }

    val accum = new AccumulableInfo(
      1L, Some(InternalAccumulator.MEMORY_BYTES_SPILLED),
      Some(1L), None, internal = true, countFailedValues = false, None)
    executorId = 0
    while (executorId < executors) {
      val updates = tasksByExecutor(executorId).iterator.map { task =>
        (task.taskId, stage.stageId, stage.attemptNumber(), Seq(accum))
      }.toSeq
      listener.onExecutorMetricsUpdate(
        SparkListenerExecutorMetricsUpdate(executorId.toString, updates))
      executorId += 1
    }

    if (finishTasks) {
      tasksByExecutor.foreach { tasks =>
        tasks.foreach { task =>
          task.markFinished(TaskState.FINISHED, 2L)
          listener.onTaskEnd(SparkListenerTaskEnd(
            stage.stageId, stage.attemptNumber(), "task", Success, task,
            new ExecutorMetrics, taskMetrics = null))
        }
      }
    }
    Fixture(listener, store, stage)
  }

  private def withFixture(
      finishTasks: Boolean,
      executors: Int = numExecutors,
      tasksPerExec: Int = tasksPerExecutor)(f: Fixture => Unit): Unit = {
    val fixture = prepare(finishTasks, executors, tasksPerExec)
    try f(fixture) finally fixture.store.close()
  }

  /** Make the next executor-metrics event flush, independent of time since the setup flush. */
  private def armFlush(listener: AppStatusListener): Unit = {
    val field = classOf[AppStatusListener].getDeclaredField("lastFlushTimeNs")
    field.setAccessible(true)
    field.setLong(listener, 0L)
  }

  /**
   * Mark every live entity dirty so the next flush or stage completion rewrites it.
   * That is the listener behavior before clean entities were skipped.
   */
  private def markAllDirty(listener: AppStatusListener): Unit = {
    Seq("liveJobs", "liveStages", "liveTasks", "liveExecutors", "liveRDDs", "pools")
      .foreach { name =>
        val field = classOf[AppStatusListener].getDeclaredField(name)
        field.setAccessible(true)
        markCollection(field.get(listener))
      }
  }

  private def markCollection(collection: Any): Unit = collection match {
    case map: java.util.Map[_, _] =>
      val iterator = map.values().iterator()
      while (iterator.hasNext) {
        markDirty(iterator.next())
      }
    case map: scala.collection.Map[_, _] =>
      map.values.foreach(markDirty)
    case _ =>
  }

  private def markDirty(value: Any): Unit = value match {
    case entity: LiveEntity =>
      entity.markDirty()
      val summaries = value.getClass.getDeclaredFields.find(_.getName == "executorSummaries")
      summaries.foreach { field =>
        field.setAccessible(true)
        markCollection(field.get(value))
      }
    case _ =>
  }
}
