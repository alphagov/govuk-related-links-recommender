import pandas as pd
import numpy as np
import os


class RelatedLinksCsvExporter:

    def __init__(self, related_links, content_id_to_base_path_mapper, content_ids_to_page_views_mapper):
        self.related_links = related_links
        self.content_id_to_base_path_mapper = content_id_to_base_path_mapper
        self.content_ids_to_page_views_mapper = content_ids_to_page_views_mapper

    def export(self, file_path):
        """
        Add two columns to pandas DataFrame giving the base_paths for the source and target content_ids
        :param df: pandas DataFrame with columns named 'source_content_id' and 'target_content_id'
        :return:
        Exports DataFrame containing links to csv
        :param file_path:
        :return:
        """

        file_extension = os.path.splitext(file_path)[1]
        sep = '\t' if file_extension == '.tsv' else ','

        row_list = [{'source_content_id': source_cid,
                     'source_base_path': self.content_id_to_base_path_mapper.get(source_cid, 'non-English base path'),
                     'target_content_id': target_cid,
                     'target_base_path': self.content_id_to_base_path_mapper.get(target_cid, 'non-English base path'),
                     'probability': prob,
                     'source_page_views': self.content_ids_to_page_views_mapper.get(source_cid, np.nan),
                     'target_page_views': self.content_ids_to_page_views_mapper.get(target_cid, np.nan)}
                    for source_cid, results in self.related_links.items() for target_cid, prob in results]
        df_with_paths = pd.DataFrame(row_list,
                                     columns=['source_base_path', 'target_base_path', 'probability',
                                              'source_content_id', 'target_content_id', 'source_page_views',
                                              'target_page_views'])
        df_with_paths.to_csv(file_path, index=False, sep=sep)
