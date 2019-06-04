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

"""Configurations of thrift logger"""

# this setting is the same as go logger used by m10n team
DEFAULT_MAX_BYTES_PER_FILE = 10 * (1 << 20)
DEFAULT_MAX_BACKUP_FILE_COUNT = 100

RAW_LOG_DIR = '/mnt/thrift_logger'
LOW_SAMPLE_RATE = .001
HIGH_SAMPLE_RATE = 1.0
DEFAULT_SAMPLE_RATE = 0.01

# These are kafka topics that we don't want to sample in statsd (they are low volume) and important
LOW_VOLUME_KAFKA_TOPICS = ['lowvolumntopic1', 'lowvolumntopic1']

# These are kafka topics that we can statsd sample at a very low rate (.001). For high volume topics,
# we may need a larger file size for thrift-logger file. We define a (topic -> expand_ratio) dictionary
# here on the thrift file size expansion ratio for these topics.
HIGH_VOLUME_KAFKA_TOPICS = {'topic1': 1, 'topic2': 2.0, 'topic3': 1}
