import json
from unittest.mock import Mock

from sdcm.db_stats import PrometheusDBStats


def _stats_with_mocked_session():
    stats = PrometheusDBStats.__new__(PrometheusDBStats)  # skip __init__, which queries the server
    stats.host, stats.port, stats.protocol = "10.0.0.1", 9090, "http"
    response = Mock()
    response.content = json.dumps({"status": "success", "data": {"result": []}}).encode()
    stats._session = Mock(get=Mock(return_value=response), post=Mock(return_value=response))
    return stats


def test_request_passes_connect_and_read_timeouts_to_get():
    stats = _stats_with_mocked_session()

    stats.request("http://10.0.0.1:9090/api/v1/query")

    assert stats._session.get.call_args.kwargs["timeout"] == (10, 130)


def test_request_passes_timeouts_to_post_too():
    stats = _stats_with_mocked_session()

    stats.request("http://10.0.0.1:9090/api/v1/query", post=True)

    assert stats._session.post.call_args.kwargs["timeout"] == (10, 130)
