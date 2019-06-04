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
"""This is a example to use logger to dump thrift data for Singer to pick up later."""

import time
from thrift_logger.thrift_logger_wrapper import Logger

if __name__ == '__main__':
    kafka_logger = Logger()
    topic1 = 'mao_topic_2'
    topic2 = 'mao_topic_3'
    kafka_logger.add_topic_logger(topic1, max_bytes=2000, backup_count=10)
    kafka_logger.add_topic_logger(topic2, max_bytes=3000, backup_count=15)

    while True:
        # get some fake bytes
        random_str = str(time.time())
        random_bytes = str.encode(random_str)
        kafka_logger.log(topic1, message=random_bytes)
        kafka_logger.log(topic2, message=random_bytes)
        time.sleep(1)
