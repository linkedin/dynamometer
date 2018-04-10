# Dynamometer [![Build Status](https://travis-ci.org/linkedin/dynamometer.svg?branch=master)](https://travis-ci.org/linkedin/dynamometer)

## Overview

Dynamometer is a tool to performance test Hadoop's HDFS NameNode. The intent is to provide a
real-world environment by initializing the NameNode against a production file system image and replaying
a production workload collected via e.g. the NameNode's audit logs. This allows for replaying a workload
which is not only similar in characteristic to that experienced in production, but actually identical.

Dynamometer will launch a YARN application which starts a single NameNode and a configurable number of
DataNodes, simulating an entire HDFS cluster as a single application. There is an additional `workload`
job run as a MapReduce job which accepts audit logs as input and uses the information contained within to
submit matching requests to the NameNode, inducing load on the service.

Dynamometer can execute this same workload against different Hadoop versions or with different
configurations, allowing for the testing of configuration tweaks and code changes at scale without the
necessity of deploying to a real large-scale cluster.

Throughout this documentation, we will use "Dyno-HDFS", "Dyno-NN", and "Dyno-DN" to refer to the HDFS
cluster, NameNode, and DataNodes (respectively) which are started _inside of_ a Dynamometer application.
Terms like HDFS, YARN, and NameNode used without qualification refer to the existing infrastructure on
top of which Dynamometer is run.

## Requirements

Dynamometer is based around YARN applications, so an existing YARN cluster will be required for execution.
It also requires an accompanying HDFS instance to store some temporary files for communication.

## Building

Dynamometer consists of three main components:
* Infrastructure: This is the YARN application which starts a Dyno-HDFS cluster.
* Workload: This is the MapReduce job which replays audit logs.
* Block Generator: This is a MapReduce job used to generate input files for each Dyno-DN; its
  execution is a prerequisite step to running the infrastructure application.

They are built through standard Gradle means, i.e. `gradle build`. In addition to compiling everything,
this will generate a distribution tarball, containing all necessary components for an end user, at
`build/distributions/dynamometer-VERSION.tar` (a zip is also generated; their contents are identical).
This distribution does not contain any Hadoop dependencies, which are necessary to launch the application,
as it assumes Dynamometer will be run from a machine which has a working installation of Hadoop. To
include Dynamometer's Hadoop dependencies, use `build/distributions/dynamometer-fat-VERSION.tar`.

## Usage

Scripts discussed below can be found in the `bin` directory of the distribution. The corresponding
Java JAR files can be found in the `lib` directory.

### Preparing Requisite Files

A number of steps are required in advance of starting your first Dyno-HDFS cluster:

* Collect an fsimage and related files from your NameNode. This will include the `fsimage_TXID` file
  which the NameNode creates as part of checkpointing, the `fsimage_TXID.md5` containing the md5 hash
  of the image, the `VERSION` file containing some metadata, and the `fsimage_TXID.xml` file which can
  be generated from the fsimage using the offline image viewer:
  ```
  hdfs oiv -i fsimage_TXID -o fsimage_TXID.xml -p XML
  ```
  It is recommended that you collect these files from your Secondary/Standby NameNode if you have one
  to avoid placing additional load on your Active NameNode.

  All of these files must be placed somewhere on HDFS where the various jobs will be able to access them.
  They should all be in the same folder, e.g. `hdfs:///dyno/fsimage`.

  All of these steps can be automated with the `upload-fsimage.sh` script, e.g.:
  ```
  ./bin/upload-fsimage.sh 0001 hdfs:///dyno/fsimage
  ```
  Where 0001 is the transaction ID of the desired fsimage. See usage info of the script for more detail.
* Collect the Hadoop distribution tarball to use to start the Dyno-NN and -DNs. For example, if
  testing against Hadoop 2.7.4, use
  [hadoop-2.7.4.tar.gz](http://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz).
  This distribution contains a number of components unnecessary for Dynamometer (e.g. YARN), so to reduce
  its size, you can optionally use the `create-slim-hadoop-tar.sh` script:
  ```
  ./bin/create-slim-hadoop-tar.sh hadoop-VERSION.tar.gz
  ```
  The Hadoop tar can be present on HDFS or locally where the client will be run from. Its path will be
  supplied to the client via the `-hadoop_binary_path` argument.

  Alternatively, if you use the `-hadoop_version` argument, you can simply specify which version you would
  like to run against (e.g. '2.7.4') and the client will attempt to download it automatically from an
  Apache mirror. See the usage information of the client for more details.
* Prepare a configuration directory. You will need to specify a configuration directory with the standard
  Hadoop configuration layout, e.g. it should contain `etc/hadoop/*-site.xml`. This determines with what
  configuration the Dyno-NN and -DNs will be launched. Configurations that must be modified for
  Dynamometer to work properly (e.g. `fs.defaultFS` or `dfs.namenode.name.dir`) will be overriden
  at execution time. This can be a directory if it is available locally, else an archive file on local
  or remote (HDFS) storage.

### Execute the Block Generation Job

This will use the `fsimage_TXID.xml` file to generate the list of blocks that each Dyno-DN should
advertise to the Dyno-NN. It runs as a MapReduce job.
```
./bin/generate-block-lists.sh
    -fsimage_input_path hdfs:///dyno/fsimage/fsimage_TXID.xml
    -block_image_output_dir hdfs:///dyno/blocks
    -num_reducers R
    -num_datanodes D
```
In this example, the XML file uploaded above is used to generate block listings into `hdfs:///dyno/blocks`.
`R` reducers are used for the job, and `D` block listings are generated - this will determine how many
Dyno-DNs are started in the Dyno-HDFS cluster.

### Prepare Audit Traces (Optional)

This step is only necessary if you intend to use the audit trace replay capabilities of Dynamometer; if you
just intend to start a Dyno-HDFS cluster you can skip to the next section.

The audit trace replay accepts one input file per mapper, and currently supports two input formats, configurable
via the `auditreplay.command-parser.class` configuration.

The default is a direct format,
`com.linkedin.dynamometer.workloadgenerator.audit.AuditLogDirectParser`. This accepts files in the format produced
by a standard configuration audit logger, e.g. lines like:
```
1970-01-01 00:00:42,000 INFO FSNamesystem.audit: allowed=true	ugi=hdfs	ip=/127.0.0.1	cmd=open	src=/tmp/foo	dst=null	perm=null	proto=rpc
```
When using this format you must also specify `auditreplay.log-start-time.ms`, which should be (in milliseconds since
the Unix epoch) the start time of the audit traces. This is needed for all mappers to agree on a single start time. For
example, if the above line was the first audit event, you would specify `auditreplay.log-start-time.ms=42000`.

The other supporter format is `com.linkedin.dynamometer.workloadgenerator.audit.AuditLogHiveTableParser`. This accepts
files in the format produced by a Hive query with output fields, in order:

* `relativeTimestamp`: event time offset, in milliseconds, from the start of the trace
* `ugi`: user information of the submitting user
* `command`: name of the command, e.g. 'open'
* `source`: source path
* `dest`: destination path
* `sourceIP`: source IP of the event

Assuming your audit logs are available in Hive, this can be produced via a Hive query looking like:
```sql
INSERT OVERWRITE DIRECTORY '${outputPath}'
SELECT (timestamp - ${startTimestamp} AS relativeTimestamp, ugi, command, source, dest, sourceIP
FROM '${auditLogTableLocation}'
WHERE timestamp >= ${startTimestamp} AND timestamp < ${endTimestamp}
DISTRIBUTE BY src
SORT BY relativeTimestamp ASC;
```

### Start the Infrastructure Application & Workload Replay

At this point you're ready to start up a Dyno-HDFS cluster and replay some workload against it! Note that the
output from the previous two steps can be reused indefinitely.

The client which launches the Dyno-HDFS YARN application can optionally launch the workload replay
job once the Dyno-HDFS cluster has fully started. This makes each replay into a single execution of the client,
enabling easy testing of various configurations. You can also launch the two separately to have more control.
Similarly, it is possible to launch Dyno-DNs for an external NameNode which is not controlled by Dynamometer/YARN.
This can be useful for testing NameNode configurations which are not yet supported (e.g. HA NameNodes). You can do
this by passing the `-namenode_servicerpc_addr` argument to the infrastructure application with a value that points
to an external NameNode's service RPC address.

#### Manual Workload Launch

First launch the infrastructure application to begin the startup of the internal HDFS cluster, e.g.:
```
./bin/start-dynamometer-cluster.sh
    -hadoop_binary hadoop-2.7.4.tar.gz
    -conf_path my-hadoop-conf
    -fs_image_dir hdfs:///fsimage
    -block_list_path hdfs:///dyno/blocks
```
This demonstrates the required arguments. You can run this with the `-help` flag to see further usage information.

The client will track the Dyno-NN's startup progress and how many Dyno-DNs it considers live. It will notify
via logging when the Dyno-NN has exited safemode and is ready for use.

At this point, a workload job (map-only MapReduce job) can be launched, e.g.:
```
./bin/start-workload.sh
    -Dauditreplay.input-path hdfs:///dyno/audit_logs/
    -Dauditreplay.num-threads 50
    -nn_uri hdfs://namenode_address:port/
    -start_time_offset 5m
    -mapper_class_name AuditReplayMapper
```
The type of workload generation is configurable; AuditReplayMapper replays an audit log trace as discussed previously.
The AuditReplayMapper is configured via configurations; `auditreplay.input-path` and `auditreplay.num-threads` are
required to specify the input path for audit log files and the number of threads per map task. A number of map tasks
equal to the number of files in `input-path` will be launched; each task will read in one of these input files and
use `num-threads` threads to replay the events contained within that file. A best effort is made to faithfully replay
the audit log events at the same pace at which they originally occurred (optionally, this can be adjusted by
specifying `auditreplay.rate-factor` which is a multiplicative factor towards the rate of replay, e.g. use 2.0 to
replay the events at twice the original speed).

#### Integrated Workload Launch

To have the infrastructure application client launch the workload automatically, parameters for the workload job
are passed to the infrastructure script. Only the AuditReplayMapper is supported in this fashion at this time. To
launch an integrated application with the same parameters as were used above, the following can be used:
```
./bin/start-dynamometer-cluster.sh
    -hadoop_binary hadoop-2.7.4.tar.gz
    -conf_path my-hadoop-conf
    -fs_image_dir hdfs:///fsimage
    -block_list_path hdfs:///dyno/blocks
    -workload_replay_enable
    -workload_input_path hdfs:///dyno/audit_logs/
    -workload_threads_per_mapper 50
    -workload_start_delay 5m
```
When run in this way, the client will automatically handle tearing down the Dyno-HDFS cluster once the
workload has completed. To see the full list of supported parameters, run this with the `-help` flag.
