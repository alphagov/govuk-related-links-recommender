import yaml
import os
import pickle


def read_file_as_string(filepath):
    """(str) -> str
    Opens the file at filepath for reading, removing /n
    before rejoining separate lines with " " separator.
    """
    with open(filepath, 'r') as file:
        lines = " ".join(line.strip("\n") for line in file)
    return lines


def parse_sql_script(path) -> str:
    """Parse a SQL script directly from the `folder` folder.
    Args:
        path: The path and filename of the SQL script.
    Returns:
        A string of the parsed SQL script.
    """
    with open(path, "r") as f:
        return f.read()


def read_config_yaml(filename):
    with open(
            os.path.join(os.path.abspath(os.path.dirname(__file__)),
                         '..', 'config', filename),
            'r') as f:
        return yaml.safe_load(f)


def load_pickled_content_id_list(filepath):
    """
    Opens a serialised python list and returns it as a list
    :param filepath:
    :return: python list
    """
    with open(filepath, "rb") as input_file:
        id_list = pickle.load(input_file)
    return id_list
