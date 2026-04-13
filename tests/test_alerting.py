import os
import unittest
from unittest.mock import Mock, patch

import requests

from service.alerting import build_failure_alert_payload, send_alert_payload


class AlertingTests(unittest.TestCase):
    def test_build_failure_alert_payload_contains_required_fields(self) -> None:
        payload = build_failure_alert_payload(
            source="azure-function",
            component="cwe_sync_timer",
            trigger="timer",
            error=RuntimeError("Databricks API failed"),
            context={"force": False, "job_id": 1001},
        )

        self.assertEqual("azure-function", payload["source"])
        self.assertEqual("cwe_sync_timer", payload["component"])
        self.assertEqual("timer", payload["trigger"])
        self.assertEqual("RuntimeError", payload["error_type"])
        self.assertEqual("Databricks API failed", payload["error_message"])
        self.assertIn("occurred_at_utc", payload)
        self.assertEqual({"force": False, "job_id": 1001}, payload["context"])
        self.assertNotIn("run_id", payload)

    def test_send_alert_payload_success(self) -> None:
        payload = {"source": "azure-function", "component": "cwe_sync_timer"}
        response = Mock()
        response.raise_for_status.return_value = None

        with patch.dict(
            os.environ,
            {
                "ALERT_ENABLED": "true",
                "ALERT_LOGICAPP_URL": "https://example.logicapp.test/hook",
                "ALERT_HTTP_TIMEOUT_SECONDS": "7",
            },
            clear=False,
        ):
            with patch("service.alerting.requests.post", return_value=response) as mocked_post:
                sent = send_alert_payload(payload)

        self.assertTrue(sent)
        mocked_post.assert_called_once_with(
            "https://example.logicapp.test/hook",
            json=payload,
            timeout=7.0,
        )

    def test_send_alert_payload_returns_false_when_request_fails(self) -> None:
        payload = {"source": "azure-function", "component": "cwe_sync_timer"}

        with patch.dict(
            os.environ,
            {
                "ALERT_ENABLED": "true",
                "ALERT_LOGICAPP_URL": "https://example.logicapp.test/hook",
                "ALERT_HTTP_TIMEOUT_SECONDS": "3",
            },
            clear=False,
        ):
            with patch(
                "service.alerting.requests.post",
                side_effect=requests.RequestException("timeout"),
            ):
                sent = send_alert_payload(payload)

        self.assertFalse(sent)

    def test_send_alert_payload_skips_when_disabled(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ALERT_ENABLED": "false",
                "ALERT_LOGICAPP_URL": "https://example.logicapp.test/hook",
            },
            clear=False,
        ):
            with patch("service.alerting.requests.post") as mocked_post:
                sent = send_alert_payload({"source": "azure-function"})

        self.assertFalse(sent)
        mocked_post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
