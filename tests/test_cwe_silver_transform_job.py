import unittest

from databricks_jobs.cwe_silver_transform_job import (
    ALL_COLUMNS,
    REMOVED_COLUMNS,
    compose_search_text,
    is_deprecated_title,
    resolve_silver_table,
    sanitize_title_for_search,
)


class CweSilverTransformJobTests(unittest.TestCase):
    def test_is_deprecated_title(self) -> None:
        self.assertTrue(is_deprecated_title("DEPRECATED: Uncontrolled File Descriptor Consumption"))
        self.assertTrue(is_deprecated_title(" deprecated: General Information Management Problems"))
        self.assertFalse(is_deprecated_title("Improper Input Validation"))

    def test_sanitize_title_for_search(self) -> None:
        self.assertEqual(
            "General Information Management Problems",
            sanitize_title_for_search("DEPRECATED: General Information Management Problems"),
        )
        self.assertEqual(
            "Improper Input Validation",
            sanitize_title_for_search("  Improper   Input\nValidation  "),
        )

    def test_compose_search_text(self) -> None:
        search_text = compose_search_text(
            weakness_id="79",
            title="DEPRECATED: Cross-site scripting",
            description="The product does not neutralize input.",
            extended_description="This allows script execution in browsers.",
            potential_mitigations='[{"description":"Use output encoding."}]',
        )

        self.assertIn("CWE-79", search_text)
        self.assertIn("Cross-site scripting", search_text)
        self.assertNotIn("DEPRECATED:", search_text)
        self.assertIn("Use output encoding.", search_text)

    def test_resolve_silver_table(self) -> None:
        self.assertEqual(
            "3dt2ndteam5.cwe.cwe_weaknesses_silver",
            resolve_silver_table(target_table="3dt2ndteam5.cwe.cwe_weaknesses"),
        )
        self.assertEqual(
            "3dt2ndteam5.cwe.custom_silver",
            resolve_silver_table(
                target_table="3dt2ndteam5.cwe.cwe_weaknesses",
                silver_table="3dt2ndteam5.cwe.custom_silver",
            ),
        )

    def test_resolve_silver_table_requires_target_when_not_explicit(self) -> None:
        with self.assertRaises(ValueError):
            resolve_silver_table(target_table="")

    def test_silver_schema_excludes_removed_metadata(self) -> None:
        self.assertIn("search_text", ALL_COLUMNS)
        for removed in REMOVED_COLUMNS:
            self.assertNotIn(removed, ALL_COLUMNS)


if __name__ == "__main__":
    unittest.main()
