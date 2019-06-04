"""Validation tests for thrift logger."""

import unittest

import mock

from thrift_logger.thrift_libs.ttypes import LogMessage
from thrift_logger.config import DEFAULT_MAX_BYTES_PER_FILE
from thrift_logger.config import DEFAULT_MAX_BACKUP_FILE_COUNT
from thrift_logger.thrift_logger import ThriftLogger


class ThriftLoggerTest(unittest.TestCase):

    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    def test_init_method(self, mkdir_mock, path_mock, rollover_mock):
        thrift_logger = ThriftLogger('topic', 'topic.11218')
        self.assertEqual(thrift_logger._backup_count, DEFAULT_MAX_BACKUP_FILE_COUNT)
        self.assertEqual(thrift_logger._max_bytes, DEFAULT_MAX_BYTES_PER_FILE)
        self.assertEqual(thrift_logger._topic, 'topic')

        thrift_logger = ThriftLogger('topic', 'topic.11218',
                                     max_bytes=10, backup_count=10)
        self.assertEqual(thrift_logger._backup_count, 10)
        self.assertEqual(thrift_logger._max_bytes, 10)
        self.assertEqual(thrift_logger._topic, 'topic')

        thrift_logger = ThriftLogger('topic', 'topic.11218',
                                     max_bytes=0, backup_count=10)
        self.assertEqual(thrift_logger._backup_count, 10)
        self.assertEqual(thrift_logger._max_bytes, DEFAULT_MAX_BYTES_PER_FILE)
        self.assertEqual(thrift_logger._topic, 'topic')

        path_mock.return_value = True
        self.assertEqual(mkdir_mock.call_count, 0)
        self.assertEqual(rollover_mock.call_count, 3)

    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    @mock.patch('__builtin__.open')
    def test_should_rollover_method(self, open_mock, mkdir_mock, path_mock, rollover_mock):
        file_mock = mock.MagicMock()
        open_mock.return_value = file_mock
        file_mock.__enter__.return_value = file_mock
        file_mock.seek.return_value = 'it doesn\'t matter'
        file_mock.tell.return_value = 20

        thrift_logger = ThriftLogger('topic', 'topic.11218')
        thrift_logger._max_bytes = 100
        thrift_logger._file_stream = None

        bytes_to_output = str.encode('dummy bytes')
        self.assertFalse(thrift_logger._should_rollover(bytes_to_output))

        thrift_logger._max_bytes = 20
        self.assertTrue(thrift_logger._should_rollover(bytes_to_output))

        path_mock.return_value = True
        self.assertEqual(mkdir_mock.call_count, 0)
        self.assertEqual(rollover_mock.call_count, 1)

    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    def test_make_log_msg_method(self, mkdir_mock, path_mock, rollover_mock):
        thrift_logger = ThriftLogger('topic', 'topic.11218')

        key = str.encode('key')
        msg = str.encode('msg')
        log_msg = thrift_logger._make_log_msg(key=key, bytes_payload=msg)

        correct_target_log_msg = LogMessage()
        correct_target_log_msg.key = key
        correct_target_log_msg.message = msg
        self.assertEqual(log_msg, correct_target_log_msg)

        wrong_target_log_msg = LogMessage()
        wrong_target_log_msg.key = key
        wrong_target_log_msg.message = str.encode('wrong_msg')
        self.assertNotEqual(log_msg, wrong_target_log_msg)

        no_key_log_msg = thrift_logger._make_log_msg(key=None, bytes_payload=msg)
        correct_target_log_msg.key = None
        self.assertEqual(no_key_log_msg, correct_target_log_msg)

        path_mock.return_value = True
        self.assertEqual(mkdir_mock.call_count, 0)
        self.assertEqual(rollover_mock.call_count, 1)

    @mock.patch('thrift.protocol.TBinaryProtocol.TBinaryProtocol')
    @mock.patch('thrift.transport.TTransport.TFramedTransport')
    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    def test_open_method(self, mkdir_mock, path_mock, rollover_mock, transport_mock, protocol_mock):
        f_mock = mock.MagicMock()
        transport_mock.return_value = f_mock
        f_mock.__enter__.return_value = f_mock

        p_mock = mock.MagicMock()
        protocol_mock.return_value = p_mock
        p_mock.__enter__.return_value = p_mock

        thrift_logger = ThriftLogger('topic', 'topic.11218')
        thrift_logger._open()
        path_mock.return_value = True
        self.assertEqual(mkdir_mock.call_count, 0)
        self.assertEqual(rollover_mock.call_count, 1)
        self.assertEqual(transport_mock.call_count, 1)
        self.assertEqual(protocol_mock.call_count, 1)
        self.assertIsNotNone(thrift_logger._file_stream)
        self.assertIsNotNone(thrift_logger._protocol)
        self.assertIsNotNone(thrift_logger._transport)

    @mock.patch('thrift_logger.thrift_libs.ttypes.LogMessage.write')
    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._should_rollover')
    @mock.patch('thrift.protocol.TBinaryProtocol.TBinaryProtocol')
    @mock.patch('thrift.transport.TTransport.TFramedTransport')
    @mock.patch('thrift_logger.thrift_logger.ThriftLogger._do_rollover')
    @mock.patch('os.path.exists')
    @mock.patch('os.makedirs')
    def test_log_method(self, mkdir_mock, path_mock, rollover_mock,
                        transport_mock, protocol_mock, should_rollover_mock,
                        log_message_write_mock):
        f_mock = mock.MagicMock()
        transport_mock.return_value = f_mock
        f_mock.__enter__.return_value = f_mock

        p_mock = mock.MagicMock()
        protocol_mock.return_value = p_mock
        p_mock.__enter__.return_value = p_mock

        thrift_logger = ThriftLogger('topic', 'topic.11218')
        thrift_logger._open()
        path_mock.return_value = True
        self.assertEqual(mkdir_mock.call_count, 0)
        self.assertEqual(rollover_mock.call_count, 1)

        should_rollover_mock.return_value = False
        msg = str.encode('message')
        thrift_logger.log(bytes_payload=msg)

        log_message_write_mock.assert_called_once_with(p_mock)
