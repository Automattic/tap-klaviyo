"""Unit tests for the events stream and its paginator."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tap_klaviyo.streams import EventsStream, KlaviyoEventsPaginator
from tap_klaviyo.tap import TapKlaviyo

current_path = Path(__file__).resolve().parent
config_path = current_path / ".." / "config.json"
SAMPLE_CONFIG = json.loads(config_path.read_text())


class FakeResponse:
    """Minimal stand-in for requests.Response used by the paginator."""

    def __init__(self, next_url):
        self._next_url = next_url

    def json(self):
        return {"links": {"next": self._next_url}}


@pytest.fixture
def events_stream():
    return EventsStream(tap=TapKlaviyo(config=SAMPLE_CONFIG, validate_config=False))


def _row(dt_value):
    return {"id": "abc", "attributes": {"datetime": dt_value}}


def test_post_process_keeps_past_event(events_stream):
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    row = events_stream.post_process(_row(past))
    assert row is not None
    assert row["datetime"] == past
    assert events_stream.last_datetime == past


def test_post_process_drops_future_event(events_stream):
    future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    assert events_stream.post_process(_row(future)) is None


def test_post_process_future_event_does_not_advance_bookmark(events_stream):
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()

    events_stream.post_process(_row(past))
    events_stream.post_process(_row(future))

    # last_datetime stays at the past value; the future record is ignored.
    assert events_stream.last_datetime == past


def test_paginator_continues_before_current_day(events_stream):
    paginator = KlaviyoEventsPaginator(events_stream)
    events_stream.last_datetime = (
        paginator.max_timestamp - timedelta(days=1)
    ).isoformat()
    next_url = "https://a.klaviyo.com/api/events?page[cursor]=xyz"
    assert paginator.get_next_url(FakeResponse(next_url)) == next_url


def test_paginator_stops_at_current_day(events_stream):
    paginator = KlaviyoEventsPaginator(events_stream)
    events_stream.last_datetime = paginator.max_timestamp.isoformat()
    assert paginator.get_next_url(FakeResponse("https://example.com/next")) is None


def test_paginator_continues_when_no_records_seen_yet(events_stream):
    paginator = KlaviyoEventsPaginator(events_stream)
    # last_datetime is None on the first page; must not raise and must paginate.
    next_url = "https://a.klaviyo.com/api/events?page[cursor]=first"
    assert paginator.get_next_url(FakeResponse(next_url)) == next_url