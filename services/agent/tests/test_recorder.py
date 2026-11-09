import asyncio
import hashlib
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from recorder import ConversationRecorder, PublicConversationPublisher, redact_public_text


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self.upload = None

    def upload_from_string(self, value, **kwargs):
        self.upload = (value, kwargs)


class FakeBucket:
    def __init__(self):
        self.blobs = []

    def blob(self, name):
        blob = FakeBlob(name)
        self.blobs.append(blob)
        return blob


class FakeClient:
    def __init__(self):
        self.bucket_name = None
        self.value = FakeBucket()

    def bucket(self, name):
        self.bucket_name = name
        return self.value


class ConversationRecorderTest(unittest.TestCase):
    def test_writes_private_eval_record_under_utc_date_and_hour(self):
        client = FakeClient()
        recorder = ConversationRecorder(
            "chat-bucket",
            client=client,
            clock=lambda: datetime(2026, 11, 6, 4, 59, 1, tzinfo=timezone.utc),
        )

        object_name = asyncio.run(
            recorder.record(
                request_id="request-123456789",
                parent_request_id="parent-123456789",
                thread_id="root-123456789",
                turn_index=2,
                session_id="session-123456789",
                question="How did 1T perform?",
                response={"answer": "It was reliable."},
                scope={"route_id": "1T", "week_end": "2026-10-31"},
                model="gemini-test",
                status="success",
                latency_ms=1234,
                prompt_features=["comparison"],
                tool_calls=[{"tool": "get_route_performance", "status": "ok"}],
            )
        )

        self.assertEqual(client.bucket_name, "chat-bucket")
        self.assertEqual(
            object_name,
            "date=2026-11-06/hour=04/20261106T045901.000000Z-"
            + hashlib.sha256(b"session-123456789:request-123456789").hexdigest()[:16]
            + ".json",
        )
        blob = client.value.blobs[0]
        content, kwargs = blob.upload
        payload = json.loads(content)
        self.assertEqual(payload["question"], "How did 1T perform?")
        self.assertEqual(payload["response"]["answer"], "It was reliable.")
        self.assertEqual(payload["parent_request_id"], "parent-123456789")
        self.assertEqual(payload["thread_id"], "root-123456789")
        self.assertEqual(payload["turn_index"], 2)
        self.assertEqual(
            payload["session_hash"],
            hashlib.sha256(b"session-123456789").hexdigest(),
        )
        self.assertNotIn("session_id", payload)
        self.assertEqual(payload["runtime"]["latency_ms"], 1234)
        self.assertEqual(kwargs["content_type"], "application/json")
        self.assertEqual(kwargs["if_generation_match"], 0)

    def test_requires_bucket(self):
        with self.assertRaises(ValueError):
            ConversationRecorder("", client=FakeClient())


class PublicConversationPublisherTest(unittest.TestCase):
    def test_writes_minimal_redacted_public_turn(self):
        client = FakeClient()
        publisher = PublicConversationPublisher(
            "public-bucket",
            client=client,
            clock=lambda: datetime(2026, 11, 6, 17, 2, 3, tzinfo=timezone.utc),
        )

        object_name = asyncio.run(
            publisher.publish(
                thread_id="root-123456789",
                turn_index=2,
                question="Email me at rider@example.com about 1T",
                response={
                    "answer": "See https://example.com and call 510-555-1212.",
                    "timeframes": [{"start_date": "2026-10-30", "end_date": "2026-11-05"}],
                    "charts": [{"type": "bar", "title": "Route 1T"}],
                    "maps": [{"title": "Route 1T delay", "geojson": {"type": "FeatureCollection", "features": []}}],
                    "request_id": "must-not-publish",
                    "active_queries": 1,
                },
                scope={"route_id": "1T", "week_end": None, "extra": "omit"},
            )
        )

        conversation_id = hashlib.sha256(b"root-123456789").hexdigest()[:16]
        self.assertEqual(
            object_name,
            "analysis/public-chats/date=2026-11-06/hour=17/"
            f"20261106T170203.000000Z-{conversation_id}-turn-02.json",
        )
        blob = client.value.blobs[0]
        content, kwargs = blob.upload
        payload = json.loads(content)
        self.assertEqual(payload["question"], "Email me at [email removed] about 1T")
        self.assertEqual(
            payload["response"]["answer"],
            "See [link removed] and call [phone number removed].",
        )
        self.assertNotIn("request_id", payload["response"])
        self.assertNotIn("active_queries", payload["response"])
        self.assertEqual("Route 1T delay", payload["response"]["maps"][0]["title"])
        self.assertNotIn("thread_id", payload)
        self.assertEqual(payload["scope"], {"route_id": "1T", "week_end": None})
        self.assertEqual(blob.cache_control, "public, max-age=31536000, immutable")
        self.assertEqual(kwargs["if_generation_match"], 0)

    def test_redacts_ip_addresses(self):
        self.assertEqual(redact_public_text("client 192.168.1.4"), "client [IP address removed]")

    def test_requires_bucket(self):
        with self.assertRaises(ValueError):
            PublicConversationPublisher("", client=FakeClient())


if __name__ == "__main__":
    unittest.main()
