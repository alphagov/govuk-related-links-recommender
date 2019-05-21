from src.models.predict_related_links import *
import pytest
import pandas as pd
from gensim.models import Word2Vec
import pandas.testing as pd_testing

data_dir = os.getenv("DATA_DIR")


@pytest.fixture(scope='session')
def excluded_target_content_ids():
    return load_pickled_content_id_list("excluded_target_content_ids.pkl")


@pytest.fixture(scope='session')
def mock_excluded_list():
    return ['d490be5f-1998-4f20-ab52-d3dd5db7fa71',
            '37e27ec1-ef3e-4b3c-a2f6-14dc42c4c162',
            'bcd1365f-8496-40f5-b9cf-06f2493e48c4',
            '23eee5eb-7e24-4a7f-bf92-112f8c8132bc',
            '708334c4-2855-4d45-b311-72a26b03529a',
            'eff0d788-3b5e-4090-8b56-aaa0a4bf3f25']

@pytest.fixture(scope='session')
def mock_included_list():
    return ['03680a95-4cd4-46e6-b6d9-ec7aa5fb988e',
            '036e63af-49ee-42e0-b2dd-65cd5acf4152',
            '036ebf91-3da7-442d-ac03-b8efbce90a8d',
            '0374ee58-fd10-4e16-840e-cdaf6bbd2955',
            '03763056-11e4-43be-a203-45daab596a87',
            '03c923d2-c790-4729-9a76-e4d7a05ecf43',
            '03f0cd6d-471d-4597-b830-0b48173a78e1',
            '03f86c99-88e5-4823-a38e-9f23343dd7be',
            '04300d86-8550-4e19-9280-ecc5601349f7',
            '043e51b8-4252-49d3-8ef8-52fa1e752892']



def test_is_target_content_id_eligible(mock_excluded_list):
    assert is_target_content_id_eligible('d490be5f-1998-4f20-ab52-d3dd5db7fa71', mock_excluded_list) is False
    assert is_target_content_id_eligible('2b3617a4-3230-46bd-b7a9-9dbea78508b4', mock_excluded_list) is True


def test_exclude_ineligible_target_content_ids(mock_excluded_list):
    df = pd.DataFrame({"target_content_id": ['23eee5eb-7e24-4a7f-bf92-112f8c8132bc',  # excluded_list
                                             '708334c4-2855-4d45-b311-72a26b03529a',  # excluded_list
                                             '2b3617a4-3230-46bd-b7a9-9dbea78508b4',  # source_id
                                             '76698ffe-70ab-4fda-be0d-755234f6d340',  # eligible
                                             'f9015c31-61c2-4504-8eb0-242cd75aee19'],  # eligible
                       "probability": [0.5, 0.6, 0.6, 0.6, 0.6]})

    pd_testing.assert_frame_equal(exclude_ineligible_target_content_ids(df,
                                                                        mock_excluded_list,
                                                                        '2b3617a4-3230-46bd-b7a9-9dbea78508b4'
                                                                        ).reset_index(drop=True),
        pd.DataFrame(
            {"target_content_id": ['76698ffe-70ab-4fda-be0d-755234f6d340',
                                   'f9015c31-61c2-4504-8eb0-242cd75aee19'
                                   ],
             "probability": [0.6, 0.6]
             }).reset_index(drop=True))


#
#
# def test_get_related_links_for_a_source_content_id_in_df():
#     get_related_links_for_a_source_content_id_in_df(source_content_id, model, excluded_target_links,
#                                                     probability_threshold=0.46)
#
#
# def test_get_related_links_for_a_source_content_id_in_list():
#     get_related_links_for_a_source_content_id_in_list(source_content_id, model, excluded_target_links,
#                                                       probability_threshold=0.46)


#
def test_export_related_links_to_json(mock_included_list, mock_excluded_list):
    instance = RelatedLinksJson(mock_included_list,
                                mock_excluded_list,
                                Word2Vec.load("tests/unit/fixtures/test_model_fixture.model")
                                )
    instance.export_related_links_to_json("tests/unit/fixtures/check_related_links.json")
    with open(
            "tests/unit/fixtures/check_related_links.json") as f:
        check = json.load(f)

    with open(
            "tests/unit/fixtures/expected_related_links.json") as f:
        expected = json.load(f)
    assert check == expected


def test_write_to_csv(mock_excluded_list):
    instance = RelatedLinksCsv(pd.read_csv("tests/unit/fixtures/expected_top100_fixture.csv"),
                               mock_excluded_list,
                               Word2Vec.load("tests/unit/fixtures/test_model_fixture.model"))
    print(instance.top100_related_links_df)
    instance.write_to_csv("tests/unit/fixtures/check_top100_links.csv")

    pd_testing.assert_frame_equal(pd.read_csv("tests/unit/fixtures/check_top100_links.csv"),
                                  pd.read_csv("tests/unit/fixtures/expected_top100_links.csv"))
