import importlib
import os
import unittest
from unittest.mock import patch

import azure.functions as func


class _FailingOrchestrator:
    def run(self, *, force, trigger):
        del force
        del trigger
        raise RuntimeError("simulated orchestrator failure")


class CweAlertingBoundaryTests(unittest.TestCase):
    @staticmethod
    def _reload_cwe_module():
        with patch.dict(os.environ, {"SKIP_CWE": "False"}, clear=False):
            import blueprint.cwe as cwe_module

            return importlib.reload(cwe_module)

    def test_timer_alerts_and_reraises_original_exception(self) -> None:
        cwe_module = self._reload_cwe_module()

        with patch.object(cwe_module, "build_orchestrator", return_value=_FailingOrchestrator()):
            with patch.object(cwe_module, "notify_failure_alert") as notify_mock:
                with self.assertRaises(RuntimeError):
                    cwe_module.cwe_sync_timer(object())

        notify_mock.assert_called_once()
        kwargs = notify_mock.call_args.kwargs
        self.assertEqual("azure-function", kwargs["source"])
        self.assertEqual("cwe_sync_timer", kwargs["component"])
        self.assertEqual("timer", kwargs["trigger"])
        self.assertEqual("RuntimeError", kwargs["error"].__class__.__name__)
        self.assertIn("context", kwargs)

    def test_manual_alerts_and_returns_http_500(self) -> None:
        cwe_module = self._reload_cwe_module()
        req = func.HttpRequest(
            method="POST",
            url="https://localhost/api/cwe-sync",
            headers={},
            params={},
            route_params={},
            body=b"{}",
        )

        with patch.object(cwe_module, "build_orchestrator", return_value=_FailingOrchestrator()):
            with patch.object(cwe_module, "notify_failure_alert") as notify_mock:
                response = cwe_module.cwe_sync_manual(req)

        self.assertEqual(500, response.status_code)
        notify_mock.assert_called_once()
        kwargs = notify_mock.call_args.kwargs
        self.assertEqual("azure-function", kwargs["source"])
        self.assertEqual("cwe_sync_manual", kwargs["component"])
        self.assertEqual("http", kwargs["trigger"])
        self.assertEqual("RuntimeError", kwargs["error"].__class__.__name__)
        self.assertIn("context", kwargs)


if __name__ == "__main__":
    unittest.main()
