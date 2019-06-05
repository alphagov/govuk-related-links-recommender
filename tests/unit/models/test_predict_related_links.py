from src.models.predict_related_links import *
import pytest
import pandas as pd
from gensim.models import Word2Vec
import pandas.testing as pd_testing
import pickle

data_dir = os.getenv("DATA_DIR")


@pytest.fixture(scope='session')
def excluded_target_content_ids():
    return load_pickled_content_id_list("excluded_target_content_ids.pkl")


@pytest.fixture(scope='session')
def mock_included_target_ids():
    with open('tests/unit/fixtures/mock_eligible_target_ids_for_predicting_links.pkl', 'rb') as fp:
        return pickle.load(fp)


@pytest.fixture(scope='session')
def mock_included_source_ids():
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


def test_is_target_content_id_eligible(mock_included_target_ids):
    assert is_target_content_id_eligible('d490be5f-1998-4f20-ab52-d3dd5db7fa71', mock_included_target_ids) is False
    assert is_target_content_id_eligible('2b3617a4-3230-46bd-b7a9-9dbea78508b4', mock_included_target_ids) is True


def test_exclude_ineligible_target_content_ids(mock_included_target_ids):
    df = pd.DataFrame({"target_content_id": ['23eee5eb-7e24-4a7f-bf92-112f8c8132bc',  # excluded_list
                                             '708334c4-2855-4d45-b311-72a26b03529a',  # excluded_list
                                             '76698ffe-70ab-4fda-be0d-755234f6d340',  # eligible
                                             'f9015c31-61c2-4504-8eb0-242cd75aee19'],  # eligible
                       "probability": [0.5, 0.6, 0.6, 0.6]})

    pd_testing.assert_frame_equal(only_include_eligible_target_content_ids(df,
                                                                           mock_included_target_ids).reset_index(drop=True),
                                  pd.DataFrame(
                                      {"target_content_id": ['76698ffe-70ab-4fda-be0d-755234f6d340',
                                                             'f9015c31-61c2-4504-8eb0-242cd75aee19'
                                                             ],
                                       "probability": [0.6, 0.6]
                                       }).reset_index(drop=True))


def test_get_related_links_for_a_source_content_id(mock_included_target_ids):
    fixture_model = Word2Vec.load("tests/unit/fixtures/test_model_fixture.model")
    assert get_related_links_for_a_source_content_id('03680a95-4cd4-46e6-b6d9-ec7aa5fb988e', fixture_model,
                                                     mock_included_target_ids) == ['d9293a00-0e80-4039-b5cd-298b5153b2a3',
                                                                             'eec5b7ac-2248-4ffc-a061-b95d9de988b3',
                                                                             '79679bb8-396b-4a18-9087-64591f0ae070',
                                                                             'a7932a94-039c-48e6-9217-42821f2b91db',
                                                                             '7184be1a-977b-4008-aa79-2dfaadadec73']

    pd_testing.assert_frame_equal(
        get_related_links_for_a_source_content_id('03680a95-4cd4-46e6-b6d9-ec7aa5fb988e', fixture_model,
                                                  mock_included_target_ids, output_type="df"),
        pd.DataFrame({"target_content_id": ['d9293a00-0e80-4039-b5cd-298b5153b2a3',
                                            'eec5b7ac-2248-4ffc-a061-b95d9de988b3',
                                            '79679bb8-396b-4a18-9087-64591f0ae070',
                                            'a7932a94-039c-48e6-9217-42821f2b91db',
                                            '7184be1a-977b-4008-aa79-2dfaadadec73'],
                      "probability": [0.9817172288894653,
                                      0.9733582139015198,
                                      0.9728978872299194,
                                      0.9707686305046082,
                                      0.9695242643356323],
                      "source_content_id": ['03680a95-4cd4-46e6-b6d9-ec7aa5fb988e',
                                            '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e',
                                            '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e',
                                            '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e',
                                            '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e']}))
    # TODO add a few more examples of source content ids without 5 rls and with low probability in the top 5


def test_export_related_links_to_json(mock_included_source_ids, mock_included_target_ids):
    fixture_model = Word2Vec.load("tests/unit/fixtures/test_model_fixture.model")
    instance = RelatedLinksJson(mock_included_source_ids,
                                mock_included_target_ids,
                                fixture_model
                                )
    instance.export_related_links_to_json("tests/unit/tmp/check_related_links.json")
    with open(
            "tests/unit/tmp/check_related_links.json") as f:
        check = json.load(f)

    with open(
            "tests/unit/fixtures/expected_related_links.json") as f:
        expected = json.load(f)
    assert check == expected


def test_write_to_csv(mock_included_target_ids):
    instance = RelatedLinksCsv(pd.read_csv("tests/unit/fixtures/expected_top100_fixture.csv"),
                               mock_included_target_ids,
                               Word2Vec.load("tests/unit/fixtures/test_model_fixture.model"))
    print(instance.top100_related_links_df)
    instance.write_to_csv("tests/unit/tmp/check_top100_links.csv")

    pd_testing.assert_frame_equal(pd.read_csv("tests/unit/tmp/check_top100_links.csv"),
                                  pd.read_csv("tests/unit/fixtures/expected_top100_links.csv"))

