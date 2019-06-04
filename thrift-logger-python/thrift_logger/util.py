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
"""This file contains the utility function for thrift_logger."""

import random

from config import DEFAULT_SAMPLE_RATE
from config import HIGH_SAMPLE_RATE
from config import HIGH_VOLUME_KAFKA_TOPICS
from config import LOW_SAMPLE_RATE
from config import LOW_VOLUME_KAFKA_TOPICS


def error_text_logging(logger, text_log, sample_rate=1.0):
    """error text logger which supports sample rate"""
    if random.random() < sample_rate:
        logger.error('[%s sample rate] ' % str(sample_rate) + text_log)


def get_sample_rate(topic):
    sample_rate = DEFAULT_SAMPLE_RATE
    if topic in HIGH_VOLUME_KAFKA_TOPICS:
        sample_rate = LOW_SAMPLE_RATE
    elif topic in LOW_VOLUME_KAFKA_TOPICS:
        sample_rate = HIGH_SAMPLE_RATE
    return sample_rate
