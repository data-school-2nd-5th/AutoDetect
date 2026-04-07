import importlib
import sys
import unittest


class LazyImportBoundaryTests(unittest.TestCase):
    def _clear_modules(self) -> None:
        for module_name in (
            "service",
            "service.cwe_runtime",
            "service.cwe_orchestrator",
            "service.cwe_parser",
            "shared",
            "shared.upload_blob",
        ):
            sys.modules.pop(module_name, None)

    def test_importing_service_cwe_parser_does_not_pull_azure_deps(self) -> None:
        self._clear_modules()

        importlib.import_module("service.cwe_parser")

        self.assertNotIn("service.cwe_runtime", sys.modules)
        self.assertNotIn("shared.upload_blob", sys.modules)

    def test_importing_shared_does_not_eager_load_upload_blob(self) -> None:
        self._clear_modules()

        importlib.import_module("shared")

        self.assertNotIn("shared.upload_blob", sys.modules)


if __name__ == "__main__":
    unittest.main()
