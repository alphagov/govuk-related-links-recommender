import json

class RelatedLinksJsonExporter:
    """
    Uses a node2vec model to create a nested list of source_content_ids and their predicted target_content_ids (up to 5)
    """
    def __init__(self, related_links):
        self.related_links = related_links

    def export(self, file_path):
        """
        Converts a nested list of source_content_ids, each with up to 5 target_content_ids to a python dict and exports
        to a json file
        :param file_path:
        :return:
        """
        related_links_json = {k:[vs for vs,_ in v] for k,v in self.related_links.items()}

        with open(file_path, 'w') as f:
            json.dump(related_links_json, f, ensure_ascii=False)

