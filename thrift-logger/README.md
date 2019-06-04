ThriftLogger is a library to log to singer from Java.

<a name="Top"></a>

* <a href="#Quick Start">Quick Start</a>
* <a href="#Overview">Overview</a>

<a name="Quick Start"></a>
## Quick Start
Usage:
    ... server init ...
    // Init default file-based implementation of singer logging.
    ThriftLoggerFactory.initialize(new File("/path/to/base/log/dir"), 10000);
    ...

    // Usage in class somewhere in the server.
    public class MyClass {
         private static ThriftLogger logger = ThriftLoggerFactory.getLogger("my_topic", 72);

         ...
         logger.append(myThriftMessage);
         ...
    }

<a name="Overview"></a>
## Overview
Singer collects local logs and sends them through Kafka
to our serving system. This Logger writes to the local
singer log.

Currently files are written and rotated based on
log file size. Logback is used to manage the file rotating process.
The output format is a TFramedTransport file containing LogMessage
thrift buffers, compatible with Singer's expected input
log format.
