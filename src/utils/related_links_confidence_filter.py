import operator as op


class RelatedLinksConfidenceFilter:

    def __init__(self, content_id_to_pageview_mapper, pageview_confidence_config={}):
        self.content_id_to_pageview_mapper = content_id_to_pageview_mapper
        self.pageview_confidence_config = pageview_confidence_config

    def apply(self, source_content_id, target_content_id_probability_pair):
        # Short-circuit if we don't have the data to to anything
        if not any(self.pageview_confidence_config) or not any(target_content_id_probability_pair):
            return target_content_id_probability_pair

        # Get max pageviews that we won't filter above
        max_pageview_threshold = max(self.pageview_confidence_config.items(), key=op.itemgetter(0))[0]

        # Get page views for source content id
        pageviews = self.content_id_to_pageview_mapper.get(source_content_id, max_pageview_threshold)

        # Short-circuit again if we don't need to do any processing on the target content ids
        # i.e. the links don't need to be evaluated at anything above the default threshold specified
        # when running node2vec
        if pageviews >= max_pageview_threshold:
            return target_content_id_probability_pair

        # Return all suggested related links where the probability is greater than confidence threshold
        for pageview_threshold, confidence_threshold in self.pageview_confidence_config.items():
            if pageviews < pageview_threshold:
                return list(filter(lambda item: item[1] >= confidence_threshold, target_content_id_probability_pair))
