# <img src="docs/icons/icon-singer-sk-small.png" alt="Singer logo" width="22" align="bottom"> &nbsp; Singer

## High-performance, reliable and extensible logging agent
Singer is a high performance logging agent for uploading logs to Kafka. 
It can also be extended to support writing to other message transporters or storage systems. 

Singer runs as a standalone process on the service boxes. It monitors the log directories 
by listening to file system events, and  uploads data once it detects new data.
Singer guarantees at least one time delivery of log messages.

### Key Features: 

- **Support thrift log format and text log format out-of-box**: 
Thrift log format provides better throughput and efficiency. We highly recommend you use thrift log format 
if your logs will not be consumed directly by humans. To facilitate thrift log format usage, we 
build a set of client libraries in Python, Java, and Go for converting text 
log messages into JSON and thrift formats. 

- **At-least-once message delivery to Kafka**: Singer will retry when it fails to upload a batch of messages.
For each log stream, Singer uses a watermark file to track its progress. When Singer restarts, 
it processes messages from the watermark position.

- **Support logging in Kubernetes as a side-car service**.
Logging in Kubernetes as a daemonset. Singer can monitor and upload loads from log directories of multiple Kubernetes pods.

- **High throughput writes to Kafka**:
Singer uses multiple layers of thread pool to achieve maximum parallelism. 
Using thrift log format, Singer can achieve >100MB/second writing throughput to Kafka from one host.
Singer can process text logs at 20MB/second. 

- **Low latency logging**: 
Singer supports configurable processing latency and batch sizes, it can achieve <5ms log uploading latency. 

- **Flexible partitioning**:
Singer provides multiple partitioners for writing data to Kafka, including locality aware partitioners
that can avoid producer traffic across availability zones and reduce data transfer costs.
Singer also supports customized partitioner. 

- **Heartbeat**:
Singer supports sending heartbeats to a kafka topic periodically based on the configuration.
This allows the users to set up central monitoring of Singer instances across fleets. 

- **Write auditing**:
Singer can write an audit message to another topic for each batch of messages that it writes
to kafka. This allow users to audit Singer kafka writes. 

- **Extensible design**: 
Singer can be easily extended to support data uploading to custom destinations. 

### Detailed design

Please see [docs/DESIGN.md](docs/DESIGN.md) on Singer design.


## Build

#### Get Singer code

```bash
git clone [git-repo-url] singer
cd singer
```

#### Build Singer binary

```bash
mvn clean package -pl singer -am -DskipTests
```

As there is no native support in JDK for file system events monitoring on Mac OSX, 
some tests that run fine in the Linux environment may fail intermittently on Mac OSX. 
Please use `-DskipTests` flag if you want to build Singer on macOS. 

#### Build thift-logger client library

```bash
mvn clean package -pl thrift-logger -am
```

#### Testing

Singer has a set of unit tests that can be run through ```mvn test package -pl singer -am```.

An end-to-end integration test that can be run through:

```bash
mvn clean package -pl singer -am 
singer/src/main/scripts/run_singer_tests.sh
```

## Quick Start

The [tutorial](tutorial) directory contains a demo that shows how to run Singer. Please see [tutorial/README.md](tutorial/README.md) for details.

## Usage

#### Use Singer client library to log data to local disk 

Singer uses `file inode + offset` as the watermark position to track its progress, 
and writes the watermark info to disk after it writes a batch of messages to kafka.
It resumes from the last watermark position after restarting. 
Because of this, Singer requires that a log stream is a sequence of append-only log files, 
and uses **file renaming** for log rotation.

**Singer does not handle log streams that use file copy and truncation for log rotation**,
because Singer cannot use `file inode + offset` to uniquely identify log messages
when a log file is copied and truncated.  


For example, we have before rotation:

 ```
 ls -li 
   1001    service.log      # service.log with inode 1001
 ```

after rotation

```
 ls -li 
 
   1001   service.log.2018-11-30   # service.log.2018-11-30 with inode 1001 (was renamed from the old service.log)
   1002   service.log              # (this was newly generated service.log)
```

For logged data in plaintext format, you can directly config Singer to upload those logs. 
Singer also support high throughput logging using thrift format. 
You can write data to local disk using `thrift-logger` library that Singer provides.
Currently Singer has thrift_logger libraries in Python, Java, Go, and C++. 

Samples on using thrift_logger libraies: 
 - Java : [thrift_logger java sample](singer/src/test/java/com/pinterest/singer/e2e/LogWriter.java) 
 - Python : [thrift_logger_python_sample](thrift-logger-python/tests/thrift_logger/test_thrift_logger_wrapper.py)

#### Config Singer to upload data from local disk to Kafka

Singer uploads data based on configuration settings. 
Singer configuration is composed of two parts: 1) `singer.properties` that configures
global Singer settings, e.g. size of thread pools, daily restart settings, 
heartbeat settings, etc. 2) log stream configuration: for each set of log streams, 
singer needs one log stream configuration to define log stream related settings. 

Please see [tutorial/etc/singer](tutorial/etc/singer) for singer configurations. 
[docs/configuration_samples/sample_kubernetes](docs/configuration_samples/sample_kubernetes) has an example
on Singer configuration for Kubernetes. 


#### Run Singer

```bash
java -server  -cp $singer_home:$singer_home/lib/*:$singer_home/singer-$version.jar  \
     -Dlog4j.configuration=log4j.prod.properties -Dsinger.config.dir=$config_dir \
     com.pinterest.singer.SingerMain
```

#### Package Singer as a debian package 

```bash
tar xzvf singer-${VERSION}-bin.tar.gz --directory $SINGER_DIR
cd $BUILD_DIR

fpm -s dir -t deb -n singer -v $VERSION --deb-upstart ../singer.upstart  \
    --deb-default ../singer.default -- .
```

#### Singer Metrics

Singer exposes metrics using [Twitter Ostrich](https://github.com/twitter/ostrich) framework. 
Singer stats can be checked using the following command. Here `2047` is the  ostrich port that 
you define in `singer.ostrichPort` configuration.

```bash
curl -s localhost:2047/stats.txt
```

## License

Singer is distributed under [Apache License, Version 2.0](LICENSE).



