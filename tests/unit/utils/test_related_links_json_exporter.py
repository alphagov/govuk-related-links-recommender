import pytest
import json

from src.utils.related_links_json_exporter import RelatedLinksJsonExporter

def test_related_links_exporter_exports_empty_json_when_related_links_empty():
    related_links = {}
    expected_related_links = json.loads(json.dumps(related_links))
    json_exporter = RelatedLinksJsonExporter(related_links)

    json_exporter.export('tests/unit/tmp/related_links_export_test_empty.json')

    with open("tests/unit/tmp/related_links_export_test_empty.json") as f:
        exported_links = json.load(f)

    assert expected_related_links == exported_links

def test_related_links_exporter_exports_related_links_dictionary_as_json_when_related_links_exist():
    related_links_with_probabilities = { "03680a95-4cd4-46e6-b6d9-ec7aa5fb988e": [
        ("036e63af-49ee-42e0-b2dd-65cd5acf4152", 0.51),
        ("036ebf91-3da7-442d-ac03-b8efbce90a8d", 0.49),
        ("0374ee58-fd10-4e16-840e-cdaf6bbd2955", 0.48)],
        "b43584db-0b4b-4d49-9a65-4d4ec42c9394": [
            ("eb771368-c26d-4519-a964-0769762b3700", 0.83)
        ]}

    expected_related_links = { "03680a95-4cd4-46e6-b6d9-ec7aa5fb988e": [
        "036e63af-49ee-42e0-b2dd-65cd5acf4152",
        "036ebf91-3da7-442d-ac03-b8efbce90a8d",
        "0374ee58-fd10-4e16-840e-cdaf6bbd2955"],
        "b43584db-0b4b-4d49-9a65-4d4ec42c9394": [
            "eb771368-c26d-4519-a964-0769762b3700"
        ]}

    expected_related_links = json.loads(json.dumps(expected_related_links))
    json_exporter = RelatedLinksJsonExporter(related_links_with_probabilities)

    json_exporter.export('tests/unit/tmp/related_links_export_test_populated.json')

    with open("tests/unit/tmp/related_links_export_test_populated.json") as f:
        exported_links = json.load(f)

    assert expected_related_links == exported_links
