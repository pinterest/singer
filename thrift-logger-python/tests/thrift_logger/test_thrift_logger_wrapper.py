"""Validation tests for thrift logger wrapper"""

import mock
import unittest

from thrift_logger.thrift_logger_wrapper import Logger
from thrift_logger.thrift_logger import ThriftLogger


class LoggerTestCase(unittest.TestCase):
    def setUp(self):
        self.kafka_logger = Logger()

    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    def test_add_topic_logger_method(self, mock_mkdir, mock_path, mock_rollover):
        topic_name = 'added_topic'
        self.kafka_logger.add_topic_logger(topic_name)
        self.assertTrue(topic_name in self.kafka_logger._LOGGER_MAP)
        mock_path.return_value = True
        self.assertEqual(mock_mkdir.call_count, 0)
        self.assertEqual(mock_rollover.call_count, 1)

        another_topic_name = 'non_existing_topic'
        self.assertFalse(another_topic_name in self.kafka_logger._LOGGER_MAP)

    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    def test_get_topic_logger_method(self, mock_mkdir, mock_path, mock_rollover):
        topic_name = 'existing_topic'
        self.kafka_logger.add_topic_logger(topic_name)
        topic_logger = self.kafka_logger.get_topic_logger(topic_name)
        isinstance(topic_logger, ThriftLogger)
        mock_path.return_value = True
        self.assertEqual(mock_mkdir.call_count, 0)
        self.assertEqual(mock_rollover.call_count, 1)

        none_topic_logger = self.kafka_logger.get_topic_logger('non_existing_topic')
        self.assertIsNone(none_topic_logger)

    @mock.patch('thrift_logger.thrift_logger.ThriftLogger.log')
    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    def test_log_method_with_none_message(self, mock_mkdir, mock_path, mock_rollover, mock_log):
        topic_name = 'existing_topic'
        self.kafka_logger.add_topic_logger(topic_name)
        self.kafka_logger.log(topic_name, None)
        mock_path.return_value = True
        self.assertEqual(mock_mkdir.call_count, 0)
        self.assertEqual(mock_rollover.call_count, 1)
        self.assertEqual(mock_log.call_count, 0)

    @mock.patch('thrift_logger.thrift_logger.ThriftLogger.log')
    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    def test_log_method_with_msg(self, mock_mkdir, mock_path, mock_rollover, mock_log):
        topic_name = 'topic_a'
        message = str.encode('message')
        self.kafka_logger.add_topic_logger(topic_name)
        self.kafka_logger.log(topic_name, message)
        mock_path.return_value = True
        self.assertEqual(mock_mkdir.call_count, 0)
        self.assertEqual(mock_rollover.call_count, 1)
        mock_log.assert_called_once_with(None, message)

    @mock.patch('thrift_logger.thrift_logger.ThriftLogger.log')
    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    def test_log_method_with_key_and_msg(self, mock_mkdir, mock_path, mock_rollover, mock_log):
        topic_name = 'topic_b'
        key = str.encode('key')
        message = str.encode('message')
        self.kafka_logger.add_topic_logger(topic_name)
        self.kafka_logger.log(topic_name, message, key)
        mock_path.return_value = True
        self.assertEqual(mock_mkdir.call_count, 0)
        self.assertEqual(mock_rollover.call_count, 1)
        mock_log.assert_called_once_with(key, message)
