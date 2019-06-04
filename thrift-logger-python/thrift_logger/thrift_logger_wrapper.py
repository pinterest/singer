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
"""The logic of thrift logger wrapper.

The usage of ThriftLogger is as follows.

from thrift_logger import thrift_logger_wrapper
kafka_logger = thrift_logger_wrapper.Logger()

# to log key,msg for topic1, do
kafka_logger.log(topic1, key, msg)

# if there are multiple topics to be logged, do
kafka_logger.log(topic1, message=msg1)
kafka_logger.log(topic2, key, msg2)
"""
import logging
import os
import sys
import util

LOG = logging.getLogger('thrift_logger.thrift_logger_wrapper')
LOG.setLevel(logging.INFO)

from config import DEFAULT_MAX_BACKUP_FILE_COUNT
from config import DEFAULT_MAX_BYTES_PER_FILE
from config import HIGH_VOLUME_KAFKA_TOPICS
from config import RAW_LOG_DIR
from thrift_logger import ThriftLogger


class Logger(object):
    """This is a wrapper class of ThriftLogger.

    Attributes:
        _LOGGER_MAP: It contains a mapping between the topic and
        its corresponding ThriftLogger instance.
    """
    _LOGGER_MAP = {}

    def add_topic_logger(self, topic,
                         max_bytes=DEFAULT_MAX_BYTES_PER_FILE,
                         backup_count=DEFAULT_MAX_BACKUP_FILE_COUNT):
        """Create and add a ThriftLogger for a given topic.

        The above actions only happen when _LOGGER_MAP doesn't
        have ThriftLogger for the given topic.
        """
        file_name = self._get_base_file_name(topic)
        thrift_logger = ThriftLogger(topic, file_name, max_bytes, backup_count)
        self._LOGGER_MAP[topic] = thrift_logger
        LOG.info('Create thrift_logger for topic %s.' % topic)

    def get_topic_logger(self, topic):
        """Get the TriftLogger for the given topic."""
        if topic in self._LOGGER_MAP:
            return self._LOGGER_MAP[topic]
        else:
            return None

    def log(self, topic, message, key=None):
        """Log key and message for the specific topic."""
        if topic not in self._LOGGER_MAP:
            if topic in HIGH_VOLUME_KAFKA_TOPICS:
                self.add_topic_logger(topic, DEFAULT_MAX_BYTES_PER_FILE * HIGH_VOLUME_KAFKA_TOPICS[topic])
            else:
                self.add_topic_logger(topic)

        if message:
            topic_logger = self.get_topic_logger(topic)
            try:
                topic_logger.log(key, message)
            except:
                util.error_text_logging(LOG, 'Thrift logger wrapper failed to log for topic %s '
                                             'due to error:\n %s' % (topic, sys.exc_info()[0]),
                                        util.get_sample_rate(topic))

    def _get_base_file_name(self, topic):
        """Construct the base file name. For logs on ngapi and ngapp, we can use the port that
         is set through sys.argv[1] as the unique identifier.

         Services such as pinlater do not pass a port number in through sys.argv[1]. As each pinlater
         service worker is a different process, we will use the process id as the unique identifier.
        """
        try:
            unique_id = sys.argv[1]
            int(unique_id)
        except (IndexError, ValueError):
            unique_id = os.getpid()

        return os.path.join(RAW_LOG_DIR, (topic + '_' + str(unique_id) + '.log'))


FILE_LOGGER = Logger()
