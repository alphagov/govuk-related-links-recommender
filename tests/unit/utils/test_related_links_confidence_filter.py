from src.utils.related_links_confidence_filter import RelatedLinksConfidenceFilter

def test_apply_returns_empty_array_when_no_related_links_are_passed_in():
    content_id_to_pageview_mapper = {
        'eb771368-c26d-4519-a964-0769762b3700': 93,
        'b43584db-0b4b-4d49-9a65-4d4ec42c9394': 740,
        '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e': 231
    }

    related_links_filter = RelatedLinksConfidenceFilter(content_id_to_pageview_mapper)

    related_links = []
    actual_related_links = related_links_filter.apply('eb771368-c26d-4519-a964-0769762b3700', related_links)

    assert related_links == actual_related_links

def test_apply_returns_related_links_when_no_pageview_to_confidence_mapper_is_set():
    related_links_filter = RelatedLinksConfidenceFilter({})

    related_links = [['0374ee58-fd10-4e16-840e-cdaf6bbd2955', 0.54], ['f958056e-401c-4a14-9017-c3d7640f1d67', 0.92]]

    actual_related_links = related_links_filter.apply('0374ee58-fd10-4e16-840e-cdaf6bbd2955', related_links)

    assert related_links == actual_related_links

def test_apply_returns_related_links_when_no_thresholds_are_set():
    content_id_to_pageview_mapper = {
        'eb771368-c26d-4519-a964-0769762b3700': 93,
        'b43584db-0b4b-4d49-9a65-4d4ec42c9394': 740,
        '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e': 231
    }

    related_links_filter = RelatedLinksConfidenceFilter(content_id_to_pageview_mapper)

    related_links = [['0374ee58-fd10-4e16-840e-cdaf6bbd2955', 0.54], ['f958056e-401c-4a14-9017-c3d7640f1d67', 0.92]]

    actual_related_links = related_links_filter.apply('eb771368-c26d-4519-a964-0769762b3700', related_links)

    assert related_links == actual_related_links


def test_apply_returns_empty_related_links_when_probabilities_fall_below_threshold():
    pageviews_confidence_config = {
        100: 0.9,
        500: 0.65
    }

    content_id_to_pageview_mapper = {
        'eb771368-c26d-4519-a964-0769762b3700': 93,
        'b43584db-0b4b-4d49-9a65-4d4ec42c9394': 740,
        '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e': 231
    }

    related_links_filter = RelatedLinksConfidenceFilter(content_id_to_pageview_mapper, pageviews_confidence_config)

    related_links = [['2a864a07-1b0d-4e38-a300-2368be51c1c3', 0.54], ['f958056e-401c-4a14-9017-c3d7640f1d67', 0.82]]

    expected_related_links = []
    actual_related_links = related_links_filter.apply('eb771368-c26d-4519-a964-0769762b3700', related_links)

    assert expected_related_links == actual_related_links


def test_apply_returns_filtered_related_links_when_links_fall_below_threshold():
    pageviews_confidence_config = {
        100: 0.9,
        500: 0.65
    }

    content_id_to_pageview_mapper = {
        'eb771368-c26d-4519-a964-0769762b3700': 93,
        'b43584db-0b4b-4d49-9a65-4d4ec42c9394': 740,
        '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e': 231
    }

    related_links = [
        ['2a864a07-1b0d-4e38-a300-2368be51c1c3', 0.54],
        ['840de5db-f1b8-4610-b3a1-f36a013b6e81', 0.73],
        ['f958056e-401c-4a14-9017-c3d7640f1d67', 0.92]]

    related_links_filter = RelatedLinksConfidenceFilter(content_id_to_pageview_mapper, pageviews_confidence_config)

    expected_related_links = [['f958056e-401c-4a14-9017-c3d7640f1d67', 0.92]]
    actual_related_links = related_links_filter.apply('eb771368-c26d-4519-a964-0769762b3700', related_links)
    assert expected_related_links == actual_related_links

    expected_related_links = [
        ['840de5db-f1b8-4610-b3a1-f36a013b6e81', 0.73],
        ['f958056e-401c-4a14-9017-c3d7640f1d67', 0.92]
    ]
    actual_related_links = related_links_filter.apply('03680a95-4cd4-46e6-b6d9-ec7aa5fb988e', related_links)
    assert expected_related_links == actual_related_links

    expected_related_links = related_links
    actual_related_links = related_links_filter.apply('b43584db-0b4b-4d49-9a65-4d4ec42c9394', related_links)
    assert expected_related_links == actual_related_links

def test_apply_returns_related_links_when_source_content_id_has_no_associated_pageviews():
    pageviews_confidence_config = {
        100: 0.9,
        500: 0.65
    }

    content_id_to_pageview_mapper = {
        'b43584db-0b4b-4d49-9a65-4d4ec42c9394': 740,
        '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e': 231
    }

    related_links = [
        ['2a864a07-1b0d-4e38-a300-2368be51c1c3', 0.54],
        ['840de5db-f1b8-4610-b3a1-f36a013b6e81', 0.73],
        ['f958056e-401c-4a14-9017-c3d7640f1d67', 0.92]]

    related_links_filter = RelatedLinksConfidenceFilter(content_id_to_pageview_mapper, pageviews_confidence_config)

    expected_related_links = related_links
    actual_related_links = related_links_filter.apply('eb771368-c26d-4519-a964-0769762b3700', related_links)
    assert expected_related_links == actual_related_links

def test_apply_returns_empty_related_links_when_input_related_links_are_empty():
    pageviews_confidence_config = {
        100: 0.9,
        500: 0.65
    }

    content_id_to_pageview_mapper = {
        'eb771368-c26d-4519-a964-0769762b3700': 93,
        'b43584db-0b4b-4d49-9a65-4d4ec42c9394': 740,
        '03680a95-4cd4-46e6-b6d9-ec7aa5fb988e': 231
    }

    related_links_filter = RelatedLinksConfidenceFilter(content_id_to_pageview_mapper, pageviews_confidence_config)

    actual_related_links = related_links_filter.apply('eb771368-c26d-4519-a964-0769762b3700', [])
    assert [] == actual_related_links
