#!/usr/bin/env python3
"""Unit tests for the relay bus scaling harness.

Standard library only. Redis is never contacted: transport tests patch the
harness's ``socket.create_connection`` seam and feed scripted RESP bytes
through an in-memory stream.
"""

from __future__ import annotations

import argparse
import io
import math
import socket
import unittest
from typing import Callable
from unittest import mock

import relay_bus_scaling as harness


class FakeStream:
    """In-memory ``makefile("rwb")`` stand-in with scripted replies.

    Writes are captured in ``written``. Reads consume ``replies``. When the
    replies are exhausted, ``on_exhausted`` is invoked (if set) and
    ``socket.timeout`` is raised, mirroring a read timeout on an idle socket.
    """

    def __init__(self, replies: bytes, on_exhausted: Callable[[], None] | None = None) -> None:
        self._replies = io.BytesIO(replies)
        self._on_exhausted = on_exhausted
        self.written = bytearray()
        self.closed = False

    def write(self, data: bytes) -> int:
        self.written.extend(data)
        return len(data)

    def flush(self) -> None:
        pass

    def read(self, size: int = -1) -> bytes:
        data = self._replies.read(size)
        if not data and size != 0:
            self._exhausted()
        return data

    def readline(self) -> bytes:
        data = self._replies.readline()
        if not data:
            self._exhausted()
        return data

    def _exhausted(self) -> None:
        if self._on_exhausted is not None:
            self._on_exhausted()
        raise socket.timeout("fake stream exhausted")

    def close(self) -> None:
        self.closed = True


class FakeSocket:
    def __init__(self, stream: FakeStream) -> None:
        self.stream = stream
        self.timeout: float | None = None
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def makefile(self, mode: str) -> FakeStream:
        assert mode == "rwb"
        return self.stream

    def shutdown(self, _how: int) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def resp_simple(text: str) -> bytes:
    return f"+{text}\r\n".encode("utf-8")


def resp_array(*items: object) -> bytes:
    frame = [f"*{len(items)}\r\n".encode("ascii")]
    for item in items:
        if isinstance(item, int):
            frame.append(f":{item}\r\n".encode("ascii"))
        else:
            raw = str(item).encode("utf-8")
            frame.append(f"${len(raw)}\r\n".encode("ascii") + raw + b"\r\n")
    return b"".join(frame)


def make_args(**overrides: object) -> argparse.Namespace:
    defaults = dict(
        mode="model",
        pods="1,2,4",
        communities=64,
        events_per_community_per_sec=100.0,
        subscribed_communities=1,
        interested_pods_per_subscribed_community=0,
        redis_url="redis://127.0.0.1:6379/0",
        redis_timeout=1.0,
        assert_scaling=True,
        min_reduction_ratio=0.95,
        max_scoped_irrelevant_pct=0.0,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def make_row(**overrides: object) -> harness.Measurement:
    defaults = dict(
        pods=4,
        old_cluster=25_600,
        old_avg_pod=6_400.0,
        old_irrelevant_pct=98.44,
        new_cluster=400,
        new_avg_pod=100.0,
        new_irrelevant_pct=0.0,
        reduction=64.0,
        published=6_400,
    )
    defaults.update(overrides)
    return harness.Measurement(**defaults)


class ScalingContractTests(unittest.TestCase):
    def test_scoped_global_channel_matches_relay_topic_shape(self) -> None:
        community = "00000000-0000-0000-0000-000000000001"
        self.assertEqual(
            harness.scoped_global_channel(community),
            "buzz:00000000-0000-0000-0000-000000000001:global",
        )

    def test_model_default_proves_sixty_four_x_reduction_for_one_two_four_pods(self) -> None:
        args = make_args()
        rows = harness.model_measurements(args, [1, 2, 4])

        self.assertEqual([row.pods for row in rows], [1, 2, 4])
        self.assertEqual([row.reduction for row in rows], [64.0, 64.0, 64.0])
        self.assertEqual([row.new_irrelevant_pct for row in rows], [0.0, 0.0, 0.0])
        harness.assert_scaling(args, rows)

    def test_model_fractional_rate_below_one_stays_exact_and_passes(self) -> None:
        # Review finding R1: a 0.5 events/s scenario has a one-pod scoped rate of
        # 0.5, which must not be truncated to zero and then fail as "no deliveries".
        args = make_args(events_per_community_per_sec=0.5)
        rows = harness.model_measurements(args, [1, 2, 4])

        self.assertEqual([row.new_cluster for row in rows], [0.5, 1.0, 2.0])
        self.assertEqual([row.old_cluster for row in rows], [32.0, 64.0, 128.0])
        self.assertEqual([row.published for row in rows], [32.0, 32.0, 32.0])
        self.assertEqual([row.reduction for row in rows], [64.0, 64.0, 64.0])
        harness.assert_scaling(args, rows)

    def test_assertion_fails_when_scoped_bus_receives_irrelevant_messages(self) -> None:
        mutant_row = make_row(
            new_cluster=25_600,
            new_avg_pod=6_400.0,
            new_irrelevant_pct=98.44,
            reduction=1.0,
        )
        with self.assertRaises(AssertionError):
            harness.assert_scaling(make_args(), [mutant_row])

    def test_assertion_fails_on_zero_scoped_deliveries_with_infinite_reduction(self) -> None:
        # A scoped subscriber that received nothing used to yield reduction=inf and
        # irrelevant=0%, which satisfied both checks. Zero deliveries is a failure.
        mutant_row = make_row(new_cluster=0, new_avg_pod=0.0, reduction=math.inf)
        with self.assertRaisesRegex(AssertionError, "pods=4"):
            harness.assert_scaling(make_args(), [mutant_row])

    def test_assertion_fails_on_true_zero_redis_delivery_with_fractional_args(self) -> None:
        # Integer zero as a Redis-mode subscriber would report it, under the same
        # fractional-rate arguments that model mode now accepts.
        mutant_row = make_row(pods=1, old_cluster=32, old_avg_pod=32.0, new_cluster=0,
                              new_avg_pod=0.0, reduction=math.inf, published=32)
        with self.assertRaisesRegex(AssertionError, "no deliveries"):
            harness.assert_scaling(make_args(events_per_community_per_sec=0.5), [mutant_row])

    def test_assertion_fails_on_zero_global_deliveries(self) -> None:
        mutant_row = make_row(old_cluster=0, old_avg_pod=0.0, reduction=0.0)
        with self.assertRaises(AssertionError):
            harness.assert_scaling(make_args(), [mutant_row])

    def test_assertion_fails_on_zero_published(self) -> None:
        mutant_row = make_row(published=0)
        with self.assertRaises(AssertionError):
            harness.assert_scaling(make_args(), [mutant_row])

    def test_assertion_fails_on_non_finite_reduction_even_with_deliveries(self) -> None:
        mutant_row = make_row(reduction=math.nan)
        with self.assertRaises(AssertionError):
            harness.assert_scaling(make_args(), [mutant_row])

    def test_wait_for_counts_rejects_non_positive_expected_total(self) -> None:
        for expected in (0, -1, -6_400):
            with self.subTest(expected=expected):
                with self.assertRaises(ValueError):
                    harness.wait_for_counts([], expected, timeout=0.01)


class ValidateArgsTests(unittest.TestCase):
    RATES = (math.nan, math.inf, -math.inf, 0.0, -100.0, 0.5, 1.5, 100.0)
    INTERESTED = (-1, 0, 3)

    @staticmethod
    def expect_valid(mode: str, rate: float, interested: int) -> bool:
        if not math.isfinite(rate) or rate <= 0:
            return False
        if mode == "redis" and rate != int(rate):
            return False
        return interested >= 0

    def test_table_over_rates_and_interested_pods_in_both_modes(self) -> None:
        for mode in ("model", "redis"):
            for rate in self.RATES:
                for interested in self.INTERESTED:
                    with self.subTest(mode=mode, rate=rate, interested=interested):
                        args = make_args(
                            mode=mode,
                            events_per_community_per_sec=rate,
                            interested_pods_per_subscribed_community=interested,
                        )
                        if self.expect_valid(mode, rate, interested):
                            self.assertEqual(harness.validate_args(args), [1, 2, 4])
                        else:
                            with self.assertRaises(ValueError):
                                harness.validate_args(args)

    def test_existing_community_and_pod_checks_still_apply(self) -> None:
        cases = dict(
            communities=0,
            subscribed_communities=0,
            pods="0,1",
        )
        for key, value in cases.items():
            with self.subTest(**{key: value}):
                with self.assertRaises(ValueError):
                    harness.validate_args(make_args(**{key: value}))
        with self.assertRaises(ValueError):
            harness.validate_args(make_args(communities=2, subscribed_communities=3))

    def test_run_in_redis_mode_rejects_invalid_input_before_connecting(self) -> None:
        invalid = (
            dict(events_per_community_per_sec=math.nan),
            dict(events_per_community_per_sec=math.inf),
            dict(events_per_community_per_sec=0.0),
            dict(events_per_community_per_sec=-100.0),
            dict(events_per_community_per_sec=1.5),
            dict(interested_pods_per_subscribed_community=-1),
        )
        for overrides in invalid:
            with self.subTest(**overrides):
                connect = mock.Mock(side_effect=AssertionError("connection must not be opened"))
                with mock.patch.object(harness.socket, "create_connection", connect):
                    with self.assertRaises(ValueError):
                        harness.run(make_args(mode="redis", **overrides))
                connect.assert_not_called()

    def test_model_mode_run_accepts_fractional_rates_without_connecting(self) -> None:
        for rate in (0.5, 2.5):
            with self.subTest(rate=rate):
                connect = mock.Mock(side_effect=AssertionError("connection must not be opened"))
                with mock.patch.object(harness.socket, "create_connection", connect):
                    with mock.patch("builtins.print"):
                        self.assertEqual(harness.run(make_args(events_per_community_per_sec=rate)), 0)
                connect.assert_not_called()


class RespCodecTests(unittest.TestCase):
    def test_encode_command_byte_exact(self) -> None:
        self.assertEqual(harness.encode_command("PING"), b"*1\r\n$4\r\nPING\r\n")
        self.assertEqual(
            harness.encode_command("SELECT", 2),
            b"*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n",
        )
        # Bulk lengths are byte lengths, not code points.
        self.assertEqual(
            harness.encode_command("PUBLISH", "c", "héllo"),
            b"*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n$6\r\nh\xc3\xa9llo\r\n",
        )

    def test_read_resp_simple_string(self) -> None:
        self.assertEqual(harness.read_resp(io.BytesIO(b"+PONG\r\n")), "PONG")

    def test_read_resp_error_raises(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "NOAUTH"):
            harness.read_resp(io.BytesIO(b"-NOAUTH Authentication required.\r\n"))

    def test_read_resp_integer(self) -> None:
        self.assertEqual(harness.read_resp(io.BytesIO(b":42\r\n")), 42)

    def test_read_resp_bulk_and_null_bulk(self) -> None:
        self.assertEqual(harness.read_resp(io.BytesIO(b"$5\r\nhello\r\n")), "hello")
        self.assertIsNone(harness.read_resp(io.BytesIO(b"$-1\r\n")))

    def test_read_resp_nested_array(self) -> None:
        frame = b"*3\r\n$7\r\nmessage\r\n*2\r\n:1\r\n$1\r\nx\r\n$-1\r\n"
        self.assertEqual(harness.read_resp(io.BytesIO(frame)), ["message", [1, "x"], None])

    def test_read_resp_malformed_terminator(self) -> None:
        with self.assertRaises(ValueError):
            harness.read_resp(io.BytesIO(b"+PONG\n"))

    def test_read_resp_short_bulk(self) -> None:
        with self.assertRaises(ValueError):
            harness.read_resp(io.BytesIO(b"$5\r\nhel\r\n"))

    def test_read_resp_unknown_prefix(self) -> None:
        with self.assertRaises(ValueError):
            harness.read_resp(io.BytesIO(b"?x\r\n"))

    def test_read_resp_eof(self) -> None:
        with self.assertRaises(EOFError):
            harness.read_resp(io.BytesIO(b""))


class RedisUrlTests(unittest.TestCase):
    def test_decodes_percent_encoded_credentials(self) -> None:
        address = harness.parse_redis_url("redis://us%40er:p%40ss%3Aw%2Frd@redis.internal:6380/2")
        self.assertEqual(address.host, "redis.internal")
        self.assertEqual(address.port, 6380)
        self.assertEqual(address.db, 2)
        self.assertEqual(address.username, "us@er")
        self.assertEqual(address.password, "p@ss:w/rd")

    def test_plain_credentials_and_defaults(self) -> None:
        address = harness.parse_redis_url("redis://:secret@localhost")
        self.assertEqual(address.host, "localhost")
        self.assertEqual(address.port, 6379)
        self.assertEqual(address.db, 0)
        self.assertIsNone(address.username)
        self.assertEqual(address.password, "secret")

    def test_no_credentials(self) -> None:
        address = harness.parse_redis_url("redis://127.0.0.1:6379/0")
        self.assertIsNone(address.username)
        self.assertIsNone(address.password)

    def test_rejects_rediss_and_other_schemes(self) -> None:
        with self.assertRaises(ValueError):
            harness.parse_redis_url("rediss://localhost:6379/0")
        with self.assertRaises(ValueError):
            harness.parse_redis_url("http://localhost:6379/0")


class FakeTransportTests(unittest.TestCase):
    ADDRESS = harness.RedisAddress(
        host="redis.internal", port=6380, db=2, username="us@er", password="p@ss:w/rd"
    )

    def test_client_writes_decoded_auth_and_select_frames(self) -> None:
        stream = FakeStream(resp_simple("OK") + resp_simple("OK") + resp_simple("PONG"))
        fake_sock = FakeSocket(stream)
        with mock.patch.object(harness.socket, "create_connection", return_value=fake_sock) as connect:
            with harness.RedisClient(self.ADDRESS, timeout=1.0) as client:
                self.assertEqual(client.command("PING"), "PONG")

        connect.assert_called_once_with(("redis.internal", 6380), timeout=1.0)
        self.assertEqual(
            bytes(stream.written),
            harness.encode_command("AUTH", "us@er", "p@ss:w/rd")
            + harness.encode_command("SELECT", 2)
            + harness.encode_command("PING"),
        )
        self.assertTrue(stream.closed)
        self.assertTrue(fake_sock.closed)

    def test_client_without_credentials_or_db_sends_nothing_on_connect(self) -> None:
        stream = FakeStream(b"")
        address = harness.RedisAddress(host="h", port=1, db=0)
        with mock.patch.object(harness.socket, "create_connection", return_value=FakeSocket(stream)):
            harness.RedisClient(address, timeout=1.0).close()
        self.assertEqual(bytes(stream.written), b"")

    def test_subscriber_authenticates_counts_and_exits_cleanly(self) -> None:
        relevant = harness.community_id(0)
        irrelevant = harness.community_id(1)
        channel = harness.scoped_global_channel(relevant)
        sub = harness.RedisSubscriber(
            pod=0,
            address=self.ADDRESS,
            channels=[channel],
            relevant_communities={relevant},
            timeout=1.0,
        )
        replies = (
            resp_simple("OK")
            + resp_simple("OK")
            + resp_array("subscribe", channel, 1)
            + resp_array("message", channel, f"{relevant}|0")
            + resp_array("message", channel, f"{irrelevant}|0")
        )
        stream = FakeStream(replies, on_exhausted=sub.stop_requested.set)
        fake_sock = FakeSocket(stream)
        with mock.patch.object(harness.socket, "create_connection", return_value=fake_sock):
            sub.run()

        self.assertIsNone(sub.error)
        self.assertTrue(sub.ready.is_set())
        self.assertEqual(sub.received, 2)
        self.assertEqual(sub.irrelevant, 1)
        self.assertEqual(
            bytes(stream.written),
            harness.encode_command("AUTH", "us@er", "p@ss:w/rd")
            + harness.encode_command("SELECT", 2)
            + harness.encode_command("SUBSCRIBE", channel),
        )
        self.assertTrue(stream.closed)
        self.assertTrue(fake_sock.closed)

    def test_subscriber_records_auth_error(self) -> None:
        sub = harness.RedisSubscriber(
            pod=0,
            address=self.ADDRESS,
            channels=["buzz:global"],
            relevant_communities=set(),
            timeout=1.0,
        )
        stream = FakeStream(b"-WRONGPASS invalid username-password pair\r\n")
        with mock.patch.object(harness.socket, "create_connection", return_value=FakeSocket(stream)):
            sub.run()
        self.assertIsInstance(sub.error, RuntimeError)
        self.assertTrue(sub.ready.is_set())
        self.assertEqual(sub.received, 0)


if __name__ == "__main__":
    unittest.main()
