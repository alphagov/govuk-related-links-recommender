import yaml
import os

def read_query(filepath):
    """(str) -> str
    Opens the file at filepath for reading, removing /n
    before rejoining seperate lines with " " seperator.
    """
    with open(filepath, 'r') as file:
        lines = " ".join(line.strip("\n") for line in file)
    return lines


def get_excluded_document_types():
    with open(
            os.path.join(os.path.abspath(os.path.dirname(__file__)),
                         '..', 'config', 'document_types_excluded_from_the_topic_taxonomy.yml'
                         ),
            'r'
    ) as f:
        return yaml.safe_load(f)['document_types']