import json
import unittest

from service.cwe_parser import parse_cwe_weaknesses


SAMPLE_XML = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Weakness_Catalog xmlns:xhtml=\"http://www.w3.org/1999/xhtml\">
  <Weaknesses>
    <Weakness ID=\"200\" Name=\"Exposure of Sensitive Information to an Unauthorized Actor\" Abstraction=\"Class\" Structure=\"Simple\" Status=\"Draft\">
      <Description>The product exposes sensitive information to an actor that is not explicitly authorized to have access to that information.</Description>
      <Extended_Description>
        <xhtml:p>There are many different kinds of mistakes that introduce information exposures.</xhtml:p>
      </Extended_Description>
      <Alternate_Terms>
        <Alternate_Term>
          <Term>Information Disclosure</Term>
          <Description>desc1</Description>
        </Alternate_Term>
        <Alternate_Term>
          <Term>Information Leak</Term>
          <Description>desc2</Description>
        </Alternate_Term>
      </Alternate_Terms>
      <Potential_Mitigations>
        <Mitigation Mitigation_ID=\"MIT-46\">
          <Phase>Architecture and Design</Phase>
          <Strategy>Separation of Privilege</Strategy>
          <Description><xhtml:p>Compartmentalize the system.</xhtml:p></Description>
        </Mitigation>
      </Potential_Mitigations>
      <Likelihood_Of_Exploit>High</Likelihood_Of_Exploit>
      <Demonstrative_Examples>
        <Demonstrative_Example Demonstrative_Example_ID=\"DX-38\">
          <Intro_Text>The following code checks validity.</Intro_Text>
          <Body_Text>Body sample</Body_Text>
        </Demonstrative_Example>
      </Demonstrative_Examples>
      <Observed_Examples>
        <Observed_Example>
          <Reference>CVE-2022-31162</Reference>
          <Description>Rust library leaks Oauth client details</Description>
          <Link>https://www.cve.org/CVERecord?id=CVE-2022-31162</Link>
        </Observed_Example>
        <Observed_Example>
          <Reference>CVE-2021-25476</Reference>
          <Description>DRM leaks pointer info</Description>
          <Link>https://www.cve.org/CVERecord?id=CVE-2021-25476</Link>
        </Observed_Example>
      </Observed_Examples>
      <Mapping_Notes>
        <Usage>Discouraged</Usage>
        <Rationale>Rationale text</Rationale>
      </Mapping_Notes>
      <Content_History>
        <Modification>
          <Modification_Date>2025-09-09</Modification_Date>
        </Modification>
        <Modification>
          <Modification_Date>2025-12-11</Modification_Date>
        </Modification>
      </Content_History>
    </Weakness>
  </Weaknesses>
</Weakness_Catalog>
"""


class ParseCweWeaknessesTests(unittest.TestCase):
    def test_extracts_required_fields_and_json_columns(self) -> None:
        rows = parse_cwe_weaknesses(SAMPLE_XML)

        self.assertEqual(1, len(rows))
        row = rows[0]

        self.assertEqual("200", row["weakness_id"])
        self.assertEqual(
            "Exposure of Sensitive Information to an Unauthorized Actor", row["title"]
        )
        self.assertIn("The product exposes sensitive information", row["description"])
        self.assertIn("different kinds of mistakes", row["extended_description"])
        self.assertEqual("High", row["likelihood_of_exploit"])
        self.assertEqual("2025-12-11", row["content_history_last_modified"])

        alternative_terms = json.loads(row["alternative_terms"])
        self.assertEqual(2, len(alternative_terms))
        self.assertEqual("Information Disclosure", alternative_terms[0]["term"])

        observed = json.loads(row["selected_observed_examples"])
        self.assertEqual(2, len(observed))
        self.assertEqual("CVE-2022-31162", observed[0]["reference"])

    def test_raises_when_modification_date_missing(self) -> None:
        xml_without_modification = SAMPLE_XML.replace(
            "<Modification_Date>2025-09-09</Modification_Date>", ""
        ).replace("<Modification_Date>2025-12-11</Modification_Date>", "")

        with self.assertRaises(ValueError):
            parse_cwe_weaknesses(xml_without_modification)


if __name__ == "__main__":
    unittest.main()
