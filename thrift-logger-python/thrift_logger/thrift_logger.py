# Copyright 2019 Pinterest, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This contains the logic for thrift logging"""

import logging
import os
import sys
import time
import util

LOG = logging.getLogger('thrift_logger.thrift_logger')
LOG.setLevel(logging.INFO)

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

from config import DEFAULT_MAX_BACKUP_FILE_COUNT
from config import DEFAULT_MAX_BYTES_PER_FILE
from thrift_libs.ttypes import LogMessage


class ThriftLogger(object):
    """ A thrift logger.

    This logger logs bytes in a thrift message envelope to local disk. Later on,
    Singer will pick up those thrift message and send them to Kafka.

    Attributes:
        _topic: name of the topic in the kafka queue for the given logged thrift data.
        _max_bytes: the maximum size of a log file.
        _backup_count: the maximum number of files to log for this given ThriftLogger.
        _base_file_dir: the directory where log files are located.
        _file_stream: stream of the log file.
        _transport: a thrift.transport
        _protocol: a thrift.protocol
    """
    def __init__(self, topic, file_name, max_bytes=DEFAULT_MAX_BYTES_PER_FILE,
                 backup_count=DEFAULT_MAX_BACKUP_FILE_COUNT):
        self._topic = topic
        self._working_file_name = file_name
        self._max_bytes = max_bytes if max_bytes > 0 else DEFAULT_MAX_BYTES_PER_FILE
        self._backup_count = backup_count if backup_count > 0 else DEFAULT_MAX_BACKUP_FILE_COUNT
        self._base_file_dir = os.path.dirname(self._working_file_name)
        if not os.path.exists(self._base_file_dir):
            os.makedirs(self._base_file_dir)
        self._file_stream = None
        self._transport = None
        self._protocol = None
        # Always do rollover when initializing the logger,
        # to avoid corrupted existing working log file.
        self._do_rollover()
        LOG.info('ThriftLogger for topic %s has been created!' % self._topic)

    def _open(self):
        """Open the IO stream.

        Make it ready to output thrift data to local disk.
        """
        try:
            # Firstly, make sure close all IO stream
            if self._protocol:
                LOG.warning("Thrift protocol already exists! Need to close and reopen it.")
                self._protocol = None
            if self._transport:
                LOG.warning("Thrift transport already exists! Need to close and reopen it.")
                self._transport.close()
                self._transport = None
            # self._file_stream should be closed by self._transport.close()
            self._file_stream = None
        except IOError as ioe:
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to open the IO stream!\n '
                                             'I/O error (%s) : %s' % (self._topic, ioe.errno, ioe.strerror))
        except:
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to open the IO stream due to '
                                             'unexpected error:\n %s' % (self._topic, sys.exc_info()[0]))

        try:
            # Secondly, open new IO stream
            self._file_stream = open(self._working_file_name, 'ab')
            self._transport = TTransport.TFramedTransport(TTransport.TFileObjectTransport(self._file_stream))
            self._protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
            self._transport.open()

        except IOError as ioe:
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to open the IO stream!\n '
                                             'I/O error (%s) : %s' % (self._topic, ioe.errno, ioe.strerror))
        except:
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to open the IO stream due to '
                                             'unexpected error:\n %s' % (self._topic, sys.exc_info()[0]))

    def _close(self):
        """Close the IO stream.
        """
        if self._protocol:
            try:
                self._protocol = None
                if self._transport:
                    self._transport.close()
                    self._transport = None
                if self._file_stream:
                    # file_stream will automatically be closed by transport.close
                    # self.file_stream.close()
                    self._file_stream = None
            except IOError as ioe:
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to close the IO stream!\n '
                                             'I/O error (%s) : %s' % (self._topic, ioe.errno, ioe.strerror))
            except:
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to close the IO stream due '
                                             'to unexpected error:\n %s' % (self._topic, sys.exc_info()[0]))

    def log(self, key=None, bytes_payload=None):
        """Log bytes_payload and its (partition) key.

        Before log the bytes_payload, there is log rotation if necessary.

        Args:
            key: partition key
            bytes_payload: bytes to be logged.
        """

        if bytes_payload:
            sample_rate = util.get_sample_rate(self._topic)
            try:
                log_msg = self._make_log_msg(key, bytes_payload)

                if log_msg is None:
                    return

                if self._should_rollover(log_msg):
                    self._do_rollover()

                log_msg.write(self._protocol)
                self._transport.flush()
            except IOError as ioe:
                # With certain sample rate, we text log this error
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to log the message!\n '
                                             'I/O error (%s) : %s' % (self._topic, ioe.errno, ioe.strerror),
                                        sample_rate)
            except:
                # With certain sample rate, we text log this error
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to log the message due to '
                                             'unexpected error:\n %s' % (self._topic, sys.exc_info()[0]),
                                        sample_rate)

    def _make_log_msg(self, key, bytes_payload):
        """Create LogMessage for given key and thrift bytes.

        We always use LogMessage to wrap the bytes_payload as this is the protocol
        defined in Singer. Singer picks up LogMessage, extracts the message and send
        it to Kafka.

        Args:
            key: partition key
            bytes_payload: thrift bytes

        Returns:
            LogMessage of which the message is the bytes_payload.
        """
        sample_rate = util.get_sample_rate(self._topic)
        log_message = LogMessage()
        try:
            log_message.message = bytes_payload
            if key:
                log_message.key = key
            return log_message
        except:
            # With certain sample rate, we text log this error
            util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to log the message due to '
                                         'unexpected error:\n %s' % (self._topic, sys.exc_info()[0]),
                                    sample_rate)
            return None

    def _should_rollover(self, log_msg):
        """
        Determine if rollover should occur.
        """
        if log_msg is None:
            return False

        if self._file_stream is None:
            try:
                self._file_stream = open(self._working_file_name, 'ab')
            except:
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to check should_rollover '
                                             'due to error:\n %s' % (self._topic, sys.exc_info()[0]))
                return False
        if self._max_bytes > 0:
            try:
                if os.path.getsize(self._working_file_name) + sys.getsizeof(log_msg) >= self._max_bytes:
                    return True
            except:
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed to check should_rollover '
                                             'due to error:\n %s' % (self._topic, sys.exc_info()[0]))
                return False
        return False

    def _do_rollover(self):
        """Do a rollover."""

        # Trying to generate a file_name which doesn't conflicts with
        # existing file_name. This whole loop won't spend much time in order
        # to generate a unique file name.
        file_name = self._gen_file_name()
        try:
            while os.path.exists(file_name):
                file_name = self._gen_file_name()
        except:
            util.error_text_logging(LOG, 'ThriftLogger for topic %s failed in _do_rollver '
                                         'due to error:\n %s' % (self._topic, sys.exc_info()[0]))

        # Clean backup log files
        self._clean_backup_files()

        # Close current open stream
        self._close()

        # Rotate current working log file, if the log file exists.
        try:
            if os.path.exists(self._working_file_name):
                os.rename(self._working_file_name, file_name)
        except:
            util.error_text_logging(LOG, 'ThriftLogger for topic %s failed in _do_rollver '
                                         'due to error:\n %s' % (self._topic, sys.exc_info()[0]))
        # Reset the stream
        self._open()

    def _gen_file_name(self):
        """Generate file name for rotated log file.

        Returns:
            filename composed by working_file_name and timestamp (in milliseconds)
        """
        return "%s.%12d" % (self._working_file_name, time.time()*1000)

    def _delete_files(self, file_list):
        """Delete all files in the given file_list."""
        for f in file_list:
            try:
                if os.path.isfile(f):
                    os.remove(f)
            except:
                util.error_text_logging(LOG, 'ThriftLogger for topic %s failed in _delete_files '
                                             'due to error:\n %s' % (self._topic, sys.exc_info()[0]))

    def _clean_backup_files(self):
        """Clean backup files for a given logged topic."""
        try:
            file_list = [os.path.join(self._base_file_dir, name) for name in os.listdir(self._base_file_dir)]
            # Get all backup_files for the given logged topic.
            # Note that there might be multiple ngapi instances running in the same machine,
            # this cleanup is per-instance level.
            backup_file_timestamp_map = {}
            for filename in file_list:
                if filename.startswith(self._working_file_name) and \
                        filename != self._working_file_name and os.path.isfile(filename):
                    # We prefer to use the file modified time to determine the age of a file.
                    # However, os.path.getmtime()'s accuracy is in millisecond, so we notice that a few
                    # files may share the same age. Note that the timestamp included in the filename
                    # is in nanosecond. So we combine them two.
                    backup_file_timestamp_map[filename] = str(os.path.getmtime(filename)) + '.' + filename

            # Delete the older backup files if there are too many backup files.
            if len(backup_file_timestamp_map) > self._backup_count:
                num_files_to_del = len(backup_file_timestamp_map) - self._backup_count
                sorted_backup_files = sorted(backup_file_timestamp_map, key=backup_file_timestamp_map.get)
                # Backup files are sorted based on their age and in descending order.
                self._delete_files(sorted_backup_files[0:num_files_to_del])
        except:
            util.error_text_logging(LOG, 'ThriftLogger for topic %s failed in _clean_backup_files '
                                         'due to error:\n %s' % (self._topic, sys.exc_info()[0]))
