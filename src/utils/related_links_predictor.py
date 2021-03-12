from tqdm import tqdm
import pandas as pd
import multiprocessing
from multiprocessing import cpu_count
import numpy as np
import logging.config
import os
from collections import ChainMap


# TODO check probability threshold is correct 0.46
# TODO check maximum 5 related links is correct


class RelatedLinksPredictor:
    """
    Uses a node2vec model to create a nested list of source_content_ids and their predicted target_content_ids (up to 5)
    :param source_content_ids: list of content_ids we can link from
    :param target_content_ids: list of content_ids we can link to
    :param model: node2vec model where model.wv.vocab.keys() are content_ids
    :param probability_threshold: number in the range [0,1] representing the similarity of two nodes.
   :param num_links: maximum number of links to recommend (optional)
    """

    def __init__(self, source_content_ids, target_content_ids, model,
                 related_links_filter, probability_threshold=0.46, num_links=5):
        self.model = model
        self.logger = logging.getLogger('related_links_predictor')
        self.eligible_source_content_ids = self._get_eligible_content_ids(source_content_ids)
        self.eligible_target_content_ids = target_content_ids
        self.probability_threshold = probability_threshold
        self.num_links = num_links
        self.related_links_filter = related_links_filter

    def predict_all_related_links(self, num_workers=cpu_count()):
        params = list(map(
            lambda source_content_id: (
                source_content_id, self.eligible_target_content_ids, self.model, self.probability_threshold,
                self.num_links, self.related_links_filter),
            self._split_content_ids(self.eligible_source_content_ids, num_workers)))
        self.logger.info(f'I\'ve got {num_workers} workers and {len(params)} chunks...')

        pool = multiprocessing.Pool(processes=num_workers)
        results = pool.starmap(_predict_related_links_for_content_ids, params)

        all_related_links = dict(ChainMap(*results))

        pool.close()

        self.logger.info(f'got {(len(all_related_links))} links')
        return all_related_links

    def _get_eligible_content_ids(self, source_content_ids):
        """
        Filter eligible content_ids to only the ones included in the trained model's vocabulary
        :param source_content_ids:
        :return:
        """
        self.logger.info("Getting eligible source content_ids")

        return [
            content_id for content_id in tqdm(
                source_content_ids, desc="eligible_content_ids"
            ) if content_id in self.model.wv.vocab.keys()
        ]

    def _split_content_ids(self, content_ids, chunks):
        """

        :param content_ids:
        :param chunks:
        :return:
        """
        return np.array_split(content_ids, chunks)


def _potential_related_links_filter(links, probability_threshold, eligible_target_content_ids):
    return links[(links['probability'] > probability_threshold) &
                 (links['target_content_id'].isin(eligible_target_content_ids))]


def _predict_related_links_for_content_ids(source_content_ids, eligible_target_content_ids, model,
                                           probability_threshold, num_links, related_links_filter):
    """
    Gets the top-5 most-probable eligible target_content_ids for a single source_content_id.
    Target_content_ids are dropped if:
        - The predicted probability between source and target is below the probability threshold
        - The target_content_id is not listed in the inclusion list
        - The source and target are the same item
        - The link is not in the top 5 (highest probabilities) for that source_id
    """

    related_links = {}

    print(f"Computing related links for {len(source_content_ids)} content_ids, worker id: {os.getpid()}")

    total_links_removed = 0

    for content_id in tqdm(source_content_ids, desc="getting related links"):
        # stick to this approach because actually interacting with the most_similar generator is
        # super slow. Dump everything to a dataframe, then filter and save list values
        potential_related_links = pd.DataFrame(model.wv.most_similar(content_id, topn=1000))
        potential_related_links.columns = ['target_content_id', 'probability']
        potential_related_links.sort_values('probability', inplace=True, ascending=False)

        potential_related_links['source_content_id'] = content_id

        target_cids_with_probabilities = _potential_related_links_filter(potential_related_links,
                                                                         probability_threshold,
                                                                         eligible_target_content_ids). \
            head(num_links)[['target_content_id', 'probability']].values.tolist()

        filtered_target_cids_with_probabilities = related_links_filter.apply(
            content_id, target_cids_with_probabilities)

        total_links_removed += len(target_cids_with_probabilities) - len(filtered_target_cids_with_probabilities)

        related_links[content_id] = filtered_target_cids_with_probabilities

    print(f"Total related links not meeting confidence: {total_links_removed}")

    return related_links
