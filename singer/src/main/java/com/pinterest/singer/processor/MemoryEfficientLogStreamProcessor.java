/**
 * Copyright 2020 Pinterest, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.singer.processor;

import java.io.IOException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamReader;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;

/**
 * Memory optimized processor that processes 1 LogMessage at a time instead of
 * buffering an entire batch in a list. This enable much smaller memory
 * footprint since only 1 message needs to be actively buffered.
 * 
 * This processor should be preferred for use if memory usage is of concern
 * and/or size of batch is large enough to not fit in heap.
 *
 */
public class MemoryEfficientLogStreamProcessor extends DefaultLogStreamProcessor {

  private static final Logger LOG = LoggerFactory
      .getLogger(MemoryEfficientLogStreamProcessor.class);

  public MemoryEfficientLogStreamProcessor(LogStream logStream,
                                           String logDecider,
                                           LogStreamReader reader,
                                           LogStreamWriter writer,
                                           int batchSize,
                                           long processingIntervalInMillisMin,
                                           long processingIntervalInMillisMax,
                                           long processingTimeSliceInMilliseconds,
                                           int logRetentionInSecs) {
    super(logStream, logDecider, reader, writer, batchSize, processingIntervalInMillisMin,
        processingIntervalInMillisMax, processingTimeSliceInMilliseconds, logRetentionInSecs);
  }

  @Override
  protected int processLogMessageBatch() throws IOException, LogStreamWriterException, TException {
    LOG.debug("Start processing a batch of log messages in log stream: {} starting at position: {}",
        logStream, committedPosition);
    LogPosition batchStartPosition = committedPosition;

    int logMessagesRead = 0;
    // Read a batch of LogMessages.
    LogMessageAndPosition logMessageAndPosition = null;
    for (int i = 0; i < this.batchSize; ++i) {
      try {
        // use a tmp variable to preserve valid last read message
        LogMessageAndPosition tmp = reader.readLogMessageAndPosition();
        if (tmp == null) {
          // We run out of LogMessage, we are done with this processing cycle.
          break;
        } else {
          logMessageAndPosition = tmp;
          logMessagesRead++;
        }
      } catch (Exception e) {
        String errorString = "Caught exception when reading the current batch of messages from "
            + logStream;
        if (logMessagesRead > 0) {
          errorString += "The last good log position is: " + logMessageAndPosition.getNextPosition()
              + ". Abort this processing cycle after sending the log messages we get so far.";
        } else {
          errorString += "Abort this processing cycle without reading any messages.";
        }
        LOG.error(errorString, e);
        // break out of the loop as we have encountered an error
        break;
      }
      // keeping writes out of try catch so as to not loose data due to write
      // errors by incorrectly skipping the checkpoint (committed position)
      // this situation can happen if there is partial write success
      if (i == 0) {
        // because there is some data to read we need to prepare the commit
        writer.startCommit();
      }
      emitMessageSizeMetrics(logStream, logMessageAndPosition.getLogMessage());
      writer.writeLogMessageToCommit(logMessageAndPosition);
    }

    if (logMessagesRead > 0) {
      // Write the batch of LogMessages.
      writer.endCommit(logMessagesRead);

      LogMessage lastMessage = logMessageAndPosition.getLogMessage();
      if (lastMessage.isSetTimestampInNanos()) {
        logStream.setLatestProcessedMessageTime(lastMessage.getTimestampInNanos() / 1000000);
      }
      // The new committed position is the position after the last written LogMessage.
      LogPosition newCommittedPosition = logMessageAndPosition.getNextPosition();

      commitLogPosition(newCommittedPosition, true);
      numOfLogMessagesCommitted += logMessagesRead;
      LOG.debug("Done processing {} log messages in LogStream {} from position {} to position {}.",
          logMessagesRead, this.logStream, batchStartPosition, committedPosition);
    } else {
      LOG.debug("Done processing log messages in LogStream {} : no new messages.", this.logStream);
    }
    return logMessagesRead;
  }

}
