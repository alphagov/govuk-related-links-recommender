from src.utils.related_links_predictor import RelatedLinksPredictor
from src.utils.related_links_confidence_filter import RelatedLinksConfidenceFilter
from gensim.models import Word2Vec
import pickle


def rounded_related_links(links, digits):
    """Takes a related links dictionary and return the same dictionary but with
    the confidence score rounded off to the number of digits parameter.

    This is used below because given the random process used to compute those
    scores, they can slightly vary with no consequences on the final output. The
    test below isn't meant to check that, so it uses this function to round off
    the scores, so that it's the actual related links result that is compared.
    
    The structure of `links` is as follows:
    
    ```
    links = {
        '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e': [
            ['d9293a00-0e80-4039-b5cd-298b5153b2a3', 0.98765],
            ['eec5b7ac-2248-4ffc-a061-b95d9de988b3', 0.97654]
        ].
        '12344a95-4cd4-46e6-b6d9-ec7aa5dg234f': [
            ['d9293a00-0e80-4039-b5cd-298b5153b2a3', 0.98765],
            ['eec5b7ac-2248-4ffc-a061-b95d9de988b3', 0.97654]
        ]
    }
    ```
    """

    rounded_off_links = {}
    for content_id in links:
        rounded_off_links[content_id] = []
        for related_link in links[content_id]:
            rounded_off_links[content_id].append([
                related_link[0],
                round(related_link[1], digits)
            ])
    return rounded_off_links


def test_predict_all_related_links_creates_expected_related_links():
    with open('tests/unit/fixtures/mock_eligible_target_ids_for_predicting_links.pkl', 'rb') as fp:
        target_cids = pickle.load(fp)

    model = Word2Vec.load("tests/unit/fixtures/test_model_fixture.model")

    confidence_filter = RelatedLinksConfidenceFilter({})
    predictor = RelatedLinksPredictor(['03680a95-4cd4-46e6-b6d9-ec7aa5fb988e'], target_cids, model, confidence_filter)
    related_links = predictor.predict_all_related_links()

    expected_related_links = {'03680a95-4cd4-46e6-b6d9-ec7aa5fb988e': [
        ['d9293a00-0e80-4039-b5cd-298b5153b2a3', 0.98],
        ['eec5b7ac-2248-4ffc-a061-b95d9de988b3', 0.97],
        ['79679bb8-396b-4a18-9087-64591f0ae070', 0.97],
        ['a7932a94-039c-48e6-9217-42821f2b91db', 0.97],
        ['7184be1a-977b-4008-aa79-2dfaadadec73', 0.97]]
    }

    assert expected_related_links == rounded_related_links(related_links, 2)
