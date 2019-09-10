import pytest
import json
from src.utils.miscellaneous import read_file_as_string

from src.utils.related_links_csv_exporter import RelatedLinksCsvExporter


def test_related_links_exporter_exports_empty_csv_when_related_links_empty():
    related_links_with_probabilities = {}

    expected_related_links_csv = read_file_as_string("tests/unit/fixtures/related_links_empty.csv")

    csv_exporter = RelatedLinksCsvExporter(related_links_with_probabilities, _content_id_to_base_path_mapper(),
                                           _content_id_to_page_view_mapper())

    csv_exporter.export('tests/unit/tmp/related_links_export_test_empty.csv')

    exported_csv = read_file_as_string("tests/unit/tmp/related_links_export_test_empty.csv")

    assert expected_related_links_csv == exported_csv


def test_related_links_exporter_exports_csv_with_base_paths_when_relatd_links_exist():
    related_links_with_probabilities = {"03680a95-4cd4-46e6-b6d9-ec7aa5fb988e": [
        ("036e63af-49ee-42e0-b2dd-65cd5acf4152", 0.51),
        ("036ebf91-3da7-442d-ac03-b8efbce90a8d", 0.49),
        ("0374ee58-fd10-4e16-840e-cdaf6bbd2955", 0.48)],
        "b43584db-0b4b-4d49-9a65-4d4ec42c9394": [
            ("eb771368-c26d-4519-a964-0769762b3700", 0.83)
        ]}

    expected_related_links_csv = read_file_as_string("tests/unit/fixtures/related_links_populated.csv")

    csv_exporter = RelatedLinksCsvExporter(related_links_with_probabilities, _content_id_to_base_path_mapper(),
                                            _content_id_to_page_view_mapper())

    csv_exporter.export('tests/unit/tmp/related_links_export_test_populated.csv')

    exported_csv = read_file_as_string("tests/unit/tmp/related_links_export_test_populated.csv")

    assert expected_related_links_csv == exported_csv


def test_related_links_exporter_exports_tsv_with_base_paths_when_relatd_links_exist():
    related_links_with_probabilities = {"03680a95-4cd4-46e6-b6d9-ec7aa5fb988e": [
        ("036e63af-49ee-42e0-b2dd-65cd5acf4152", 0.51),
        ("036ebf91-3da7-442d-ac03-b8efbce90a8d", 0.49),
        ("0374ee58-fd10-4e16-840e-cdaf6bbd2955", 0.48)],
        "b43584db-0b4b-4d49-9a65-4d4ec42c9394": [
            ("eb771368-c26d-4519-a964-0769762b3700", 0.83)
        ]}

    expected_related_links_csv = read_file_as_string("tests/unit/fixtures/related_links_populated.tsv")

    csv_exporter = RelatedLinksCsvExporter(related_links_with_probabilities, _content_id_to_base_path_mapper(),
                                            _content_id_to_page_view_mapper())

    csv_exporter.export('tests/unit/tmp/related_links_export_test_populated.tsv')

    exported_csv = read_file_as_string("tests/unit/tmp/related_links_export_test_populated.tsv")

    assert expected_related_links_csv == exported_csv


def _content_id_to_base_path_mapper():
    return json.loads(json.dumps({
        "03680a95-4cd4-46e6-b6d9-ec7aa5fb988e": "/",
        "036e63af-49ee-42e0-b2dd-65cd5acf4152": "/about-government",
        "036ebf91-3da7-442d-ac03-b8efbce90a8d": "/how-government-works",
        "0374ee58-fd10-4e16-840e-cdaf6bbd2955": "/the-government-departments",
        "b43584db-0b4b-4d49-9a65-4d4ec42c9394": "/people-in-government",
        "eb771368-c26d-4519-a964-0769762b3700": "/the-role-of-government",
    }))


def _content_id_to_page_view_mapper():
    return {
        "03680a95-4cd4-46e6-b6d9-ec7aa5fb988e": 418,
        "036e63af-49ee-42e0-b2dd-65cd5acf4152": 83,
        "036ebf91-3da7-442d-ac03-b8efbce90a8d": 19,
        "0374ee58-fd10-4e16-840e-cdaf6bbd2955": 126,
        "b43584db-0b4b-4d49-9a65-4d4ec42c9394": 420,
        "eb771368-c26d-4519-a964-0769762b3700": 34,
    }
