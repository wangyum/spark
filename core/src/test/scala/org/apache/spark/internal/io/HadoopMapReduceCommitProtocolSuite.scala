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

package org.apache.spark.internal.io

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, MRJobConfig, TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.{LocalSparkContext, SparkContext, SparkFunSuite}

/**
 * Unit tests for [[HadoopMapReduceCommitProtocol]]'s dynamic partition overwrite commit path,
 * in particular the `setOnPartitionsReadyToCommit` pre-commit hook used by SPARK-38230's fast
 * path for `assumeDefaultPartitionLocations`.
 *
 * Task-side calls (`setupTask`, `newTaskTempFile`, `commitTask`) are driven through a real local
 * `SparkContext` rather than invoked directly, because `commitTask` coordinates with the driver
 * via `SparkEnv.get.outputCommitCoordinator`, which requires a live `SparkEnv`/`TaskContext`.
 * This also exercises the committer the same way production does: serialized to, and executed
 * on, a (local) executor.
 */
private object HadoopMapReduceCommitProtocolSuite {
  val jobTrackerId = "2026071202143_0101"

  def newTaskContext(stagingDir: Path, taskAttempt: String): TaskAttemptContextImpl = {
    val job = Job.getInstance(new Configuration(false))
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, stagingDir)
    val conf = job.getConfiguration
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttempt)
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1)
    new TaskAttemptContextImpl(conf, TaskAttemptID.forName(taskAttempt))
  }

  def writeToPartition(
      committer: HadoopMapReduceCommitProtocol,
      stagingDir: Path,
      taskAttempt: String,
      partition: String): FileCommitProtocol.TaskCommitMessage = {
    val tContext = newTaskContext(stagingDir, taskAttempt)
    committer.setupTask(tContext)
    val spec = FileNameSpec("", ".txt")
    val tmp = new Path(committer.newTaskTempFile(tContext, Some(partition), spec))
    val fs = tmp.getFileSystem(tContext.getConfiguration)
    fs.mkdirs(tmp.getParent)
    val out = fs.create(tmp)
    try {
      out.writeUTF("x")
    } finally {
      out.close()
    }
    committer.commitTask(tContext)
  }
}

class HadoopMapReduceCommitProtocolSuite extends SparkFunSuite with LocalSparkContext {
  import HadoopMapReduceCommitProtocolSuite._

  test("SPARK-38230: partitions-ready hook fires with the union of partition paths before " +
      "they are promoted to their final location") {
    sc = new SparkContext("local[2]", "HadoopMapReduceCommitProtocolSuite")
    withTempDir { tableDir =>
      val tableRoot = new Path(tableDir.toURI)
      val stagingDir = FileCommitProtocol.getStagingDir(tableRoot.toString, jobTrackerId)
      val committer = new HadoopMapReduceCommitProtocol(jobTrackerId, tableRoot.toString, true)

      val jobContext = newTaskContext(stagingDir, "attempt_" + jobTrackerId + "_m_000000_0")
      committer.setupJob(jobContext)

      val partitions = Seq("a=1", "a=2")
      val commits = sc.parallelize(partitions.zipWithIndex, 2).map { case (partition, idx) =>
        val taskAttempt = f"attempt_${jobTrackerId}_m_$idx%06d_0"
        writeToPartition(committer, stagingDir, taskAttempt, partition)
      }.collect().toSeq

      var observed: Set[String] = null
      committer.setOnPartitionsReadyToCommit { parts =>
        observed = parts
        assert(!new File(tableDir, "a=1").exists() && !new File(tableDir, "a=2").exists(),
          "the hook must fire before partitions are promoted to their final location")
      }

      committer.commitJob(jobContext, commits)

      assert(observed == Set("a=1", "a=2"))
      assert(new File(tableDir, "a=1").exists() && new File(tableDir, "a=2").exists(),
        "partitions must be promoted to their final location once commitJob succeeds")
    }
  }

  test("SPARK-38230: a throwing partitions-ready hook aborts the commit before any output " +
      "becomes visible at the final location") {
    sc = new SparkContext("local[1]", "HadoopMapReduceCommitProtocolSuite")
    withTempDir { tableDir =>
      val tableRoot = new Path(tableDir.toURI)
      val stagingDir = FileCommitProtocol.getStagingDir(tableRoot.toString, jobTrackerId)
      val committer = new HadoopMapReduceCommitProtocol(jobTrackerId, tableRoot.toString, true)

      val jobContext = newTaskContext(stagingDir, "attempt_" + jobTrackerId + "_m_000000_0")
      committer.setupJob(jobContext)

      val taskAttempt = "attempt_" + jobTrackerId + "_m_000000_0"
      val commit = sc.parallelize(Seq("a=1"), 1).map { partition =>
        writeToPartition(committer, stagingDir, taskAttempt, partition)
      }.collect().head

      committer.setOnPartitionsReadyToCommit { _ =>
        throw new RuntimeException("conflicting custom partition location detected")
      }

      intercept[RuntimeException] {
        committer.commitJob(jobContext, Seq(commit))
      }

      assert(!new File(tableDir, "a=1").exists(),
        "a rejected commit must not promote any partition to the final table location")
    }
  }

  test("SPARK-38230: supportsPreCommitPartitionValidation automatically disables for " +
      "any subclass, even one that touches none of the overridable methods") {
    val standardCommitter = new HadoopMapReduceCommitProtocol("job1", "/tmp", true)
    assert(standardCommitter.supportsPreCommitPartitionValidation,
      "standard HadoopMapReduceCommitProtocol must support pre-commit partition validation")

    // This subclass overrides nothing at all -- not commitJob, not any other method. It is
    // behaviorally identical to HadoopMapReduceCommitProtocol. The exact-class check in
    // supportsPreCommitPartitionValidation still must reject it, proving the restriction is
    // based on class identity alone, not on whether commitJob (or anything else) was touched.
    class PlainSubclass(jobId: String, path: String, dynamicOverwrite: Boolean)
      extends HadoopMapReduceCommitProtocol(jobId, path, dynamicOverwrite)

    val plainSubclassCommitter = new PlainSubclass("job1b", "/tmp", true)
    assert(!plainSubclassCommitter.supportsPreCommitPartitionValidation,
      "a subclass must not inherit supportsPreCommitPartitionValidation even when it overrides " +
        "nothing, since the check is based on exact class identity, not on which methods were " +
        "overridden")

    class OverridingCommitter(jobId: String, path: String, dynamicOverwrite: Boolean)
      extends HadoopMapReduceCommitProtocol(jobId, path, dynamicOverwrite) {
      override def commitJob(
          jobContext: org.apache.hadoop.mapreduce.JobContext,
          taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {}
    }

    val overridingCommitter = new OverridingCommitter("job2", "/tmp", true)
    assert(!overridingCommitter.supportsPreCommitPartitionValidation,
      "subclasses overriding commitJob must not inherit supportsPreCommitPartitionValidation")

    class ExplicitlySupportingCommitter(jobId: String, path: String, dynamicOverwrite: Boolean)
      extends OverridingCommitter(jobId, path, dynamicOverwrite) {
      override def supportsPreCommitPartitionValidation: Boolean = true
    }

    val explicitCommitter = new ExplicitlySupportingCommitter("job3", "/tmp", true)
    assert(explicitCommitter.supportsPreCommitPartitionValidation,
      "subclasses explicitly declaring supportsPreCommitPartitionValidation must be respected")
  }
}
