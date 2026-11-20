# Apache Spark

Spark is a unified analytics engine for large-scale data processing. It provides
high-level APIs in Scala, Java, Python, and R (Deprecated), and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing,
and Structured Streaming for stream processing.

- Official version: <https://spark.apache.org/>
- Development version: <https://apache.github.io/spark/>

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.spark/spark-core_2.13.svg?filter=!*preview*)](https://search.maven.org/search?q=g:org.apache.spark)
[![Java](https://img.shields.io/badge/Java-21+-orange.svg)](https://adoptium.net/temurin/releases/?version=21)
[![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_main.yml)
[![PySpark Coverage](https://codecov.io/gh/apache/spark/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/spark)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/pyspark?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads)](https://pypi.org/project/pyspark/)


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](https://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## Build Pipeline Status

See the [Apache CI dashboard](https://ci-test.apache.org/#/project/spark) for an overview
of Spark's CI usage.

| Branch     | Status                                                                                                                                                                                                          |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| master     | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/release.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/release.yml)                                               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/publish_snapshot.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/publish_snapshot.yml)                             |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_infra_images_cache.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_infra_images_cache.yml)             |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_java21.yml)                                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java25.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_java25.yml)                                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_codegen_jdk.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_codegen_jdk.yml)                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_uds.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_uds.yml)                                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_rockdb_as_ui_backend.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_rockdb_as_ui_backend.yml)         |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven.yml)                                       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java25.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java25.yml)                         |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21_macos26.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java21_macos26.yml)         |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21_arm.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java21_arm.yml)                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_coverage.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_coverage.yml)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml)                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.12_classic_only.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.12_classic_only.yml) |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.12_arm.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.12_arm.yml)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.12_macos26.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.12_macos26.yml)           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.12_pandas_3.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.12_pandas_3.yml)         |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.13.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.13.yml)                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml)                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14_nogil.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.14_nogil.yml)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.15.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.15.yml)                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_minimum.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_minimum.yml)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_connect40.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_connect40.yml)                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_connect.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_connect.yml)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_sparkr_window.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_sparkr_window.yml)                       |
| branch-4.x | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.x)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.x)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.x)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java25.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_java25.yml?query=branch%3Abranch-4.x)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.x)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.x)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.x)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.x)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml?query=branch%3Abranch-4.x)           |
| branch-4.3 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.3)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.3)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.3)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java25.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_java25.yml?query=branch%3Abranch-4.3)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.3)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.3)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.3)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.3)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml?query=branch%3Abranch-4.3)           |
| branch-4.2 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.2)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.2)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.2)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java25.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_java25.yml?query=branch%3Abranch-4.2)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.2)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.2)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.2)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.2)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml?query=branch%3Abranch-4.2)           |
| branch-4.1 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.1)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.1)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.1)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.1)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.1)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.1)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.1)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml?query=branch%3Abranch-4.1)           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml?query=branch%3Abranch-4.1) |
| branch-4.0 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.0)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.0)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.0)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.0)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.0)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.0)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.0)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml?query=branch%3Abranch-4.0) |
| branch-3.5 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-3.5)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-3.5)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_scala213.yml/badge.svg?branch=branch-3.5)](https://github.com/apache/spark/actions/workflows/build_scala213.yml?query=branch%3Abranch-3.5)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.9.yml/badge.svg?branch=branch-3.5)](https://github.com/apache/spark/actions/workflows/build_python_3.9.yml?query=branch%3Abranch-3.5)                   |


## Building Spark

Spark is built using [Apache Maven](https://maven.apache.org/).
To build Spark and its example programs, run:

```bash
./build/mvn -DskipTests clean package
```

(You do not need to do this if you downloaded a pre-built package.)

More detailed documentation is available from the project site, at
["Building Spark"](https://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see ["Useful Developer Tools"](https://spark.apache.org/developer-tools.html).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

```bash
./bin/spark-shell
```

Try the following command, which should return 1,000,000,000:

```scala
scala> spark.range(1000 * 1000 * 1000).count()
```

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

```bash
./bin/pyspark
```

And run the following command, which should also return 1,000,000,000:

```python
>>> spark.range(1000 * 1000 * 1000).count()
```

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

```bash
./bin/run-example SparkPi
```

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

```bash
MASTER=spark://host:7077 ./bin/run-example SparkPi
```

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

```bash
./dev/run-tests
```

Please see the guidance on how to
[run tests for a module, or individual tests](https://spark.apache.org/developer-tools.html#individual-tests).

There is also a Kubernetes integration test, see resource-managers/kubernetes/integration-tests/README.md

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version and Enabling YARN"](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.


Blocking/Important (must fix before merge)

1. [blocking] OOM risk: boxed-key range index budget is a lie for String/Binary/wide-Decimal keys
   RangeGroupProbe.scala:72 — Spec.keyOverhead/estimatedRetainedBytes/isEligible estimate memory as a flat 16–32 bytes/row constant for every key type. But RangeRelation.scala's IntervalIndex/PointIndex
   deep-copy String/Binary key values via InternalRow.copyValue regardless of the copyRows=false zero-copy flag (that flag only covers whole-row retention, not boxed key values). Example: a.k = b.k AND
   a.s BETWEEN b.lo AND b.hi with 2KB string bounds, a skewed key with 200K buffered rows → budget check says ~8MB (under the 32MB cap), but the actual build allocates ~800MB before the real-size check
   can reject it. This is on SortMergeJoinExec's default-on hot path. Fix: derive the per-row key-size estimate from the group's already-measured real bytes
   (ExternalAppendOnlyUnsafeRowArray.inMemoryBufferByteSize, added by the follow-up but currently unused — see finding 5) instead of a fixed constant, for the !usesLongKeys branch.

2. [important] JIRA SPARK-24020 is Resolved/Incomplete with no fixVersion
   Confirmed live via JIRA REST API: status=Resolved, resolution=Incomplete, fixVersions=[], last touched in 2021's stale-issue sweep. It traces back to a 2018 PR (apache/spark#21109) that was closed
   unmerged in 2020, and its original description proposes a completely different design (useInnerRangeOptimization, a sliding-window structure) than what's actually shipped. Needs to be
   reopened/retargeted (or replaced with a fresh ticket per this repo's CLAUDE.md) before merge so merge_spark_pr.py and release notes work correctly.

3. [important] Migration guide entry has the wrong version/section
   docs/sql-migration-guide.md:29 says "Since Spark 5.0" under the "4.4 to 5.0" section, but both new SQLConfs are tagged .version("4.5.0"), and dev/next_version_candidates.py confirms branch-4.x=4.5.0
   vs master=5.0.0 — meaning this should ship in 4.5 first. branch-4.x's guide has no "4.4 to 4.5" section yet. Found exact precedent in repo history (9cc70424b19, 789bf76b1b9) for this move-and-reword
   fix.

4. [important] Build-time exceptions are indistinguishable from normal fallback in metrics
   RangeGroupProbe.scala:192 — onProbe calls build(matches, spec, this), never passing a real buildErrorMetric, so the NonFatal catch's buildErrorMetric += 1 is permanently dead. Both "internal bug
   threw" and "normal decline (below threshold/over maxBytes/spill-backed)" land in the same numRangeIndexFallbackGroups counter — an operator can't tell them apart from SQL UI metrics. Needs a dedicated
   numRangeIndexBuildErrors metric wired end-to-end.

5. [important] inMemoryBufferByteSize is dead code with a misleading doc comment
   ExternalAppendOnlyUnsafeRowArray.scala:116-123 — added by the follow-up, doc says it lets callers budget against "real footprint instead of a schema-based guess," but it has zero callers anywhere
   (grep confirms exactly one hit: its own definition). This is the same accessor that would fix finding #1 if wired in — flagged independently by three review dimensions.

Suggestions (non-blocking)

6. [nit] RangeGroupProbe.PROBE_THRESHOLD (hardcoded 128-probe reuse gate) and Spec.threshold (user-configurable row-count gate) both read as "threshold," causing debugging confusion — rename the
   former.
7. [plausible, process] This is a default-on change to SortMergeJoinExec's hot path across all join types — worth confirming it had appropriate dev@/SPIP-level review given the blast radius, though
   this can't be verified from the code alone.

What held up well

The zero-copy design itself was independently traced and found sound: copyRows=false only applies when !isSpillBacked (so it never races against the mutable-row-reuse spill iterator), and both the
interpreted and codegen paths call resetGroup() strictly before bufferedMatches.clear(), so the index can't dangle against a cleared buffer.
