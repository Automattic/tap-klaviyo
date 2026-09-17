"""Unit tests for the shared KlaviyoStream REST client behaviour."""
import pytest
import requests
from singer_sdk.exceptions import RetriableAPIError

from tap_klaviyo.streams import CampaignsStream
from tap_klaviyo.tap import TapKlaviyo

MINIMAL_CONFIG = {"auth_token": "dummy-token"}
RETRY_AFTER_SECONDS = 17
DEFAULT_RETRY_AFTER_SECONDS = 60


@pytest.fixture
def stream():
    return CampaignsStream(tap=TapKlaviyo(config=MINIMAL_CONFIG, validate_config=False))


def _primed_wait_generator(stream):
    """Return the stream's wait generator, primed the way the backoff decorator does."""
    generator = stream.backoff_wait_generator()
    next(generator)
    return generator


def _response(headers=None):
    response = requests.Response()
    response.headers.update(headers or {})
    return response


def test_wait_honors_retry_after_header(stream):
    retry_after = {"Retry-After": str(RETRY_AFTER_SECONDS)}
    error = RetriableAPIError("429 Too Many Requests", _response(retry_after))
    assert _primed_wait_generator(stream).send(error) == RETRY_AFTER_SECONDS


def test_wait_falls_back_to_default_when_header_is_missing(stream):
    error = RetriableAPIError("500 Internal Server Error", _response())
    assert _primed_wait_generator(stream).send(error) == DEFAULT_RETRY_AFTER_SECONDS


def test_wait_falls_back_to_default_for_responseless_errors(stream):
    # The SDK also retries transport-level errors, which have no response to read a
    # Retry-After header from. Reading one anyway used to raise AttributeError and
    # kill the tap mid-sync.
    error = requests.exceptions.ChunkedEncodingError("Response ended prematurely")
    assert error.response is None
    assert _primed_wait_generator(stream).send(error) == DEFAULT_RETRY_AFTER_SECONDS


def test_wait_handles_a_connection_error(stream):
    error = requests.exceptions.ConnectionError("Connection reset by peer")
    assert _primed_wait_generator(stream).send(error) == DEFAULT_RETRY_AFTER_SECONDS
