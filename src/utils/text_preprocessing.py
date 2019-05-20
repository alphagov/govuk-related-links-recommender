from bs4 import BeautifulSoup
from itertools import chain


def is_html(text):
    """
    Checks whether text is html or not
    :param text: string
    :return: bool
    """
    try:
        return bool(BeautifulSoup(text, "html.parser").find())
    # might be fine to except all exceptions here, as it's a low-level function
    except Exception:
        return False


def extract_links_from_html(text):
    """
    Grab any GOV.UK domain-specific links from page text (looks for a href tags)
    :param text: html
    :return: list of page_paths (empty list if there are no links)
    """
    links = []
    try:
        soup = BeautifulSoup(text, 'html5lib')
        links = [link.get('href') for link in soup.findAll('a', href=True)]
    # might be fine to except all exceptions here, as it's a low-level function
    except Exception:
        None
    return [link.replace('https://www.gov.uk/', '/') for link in links
            if (link.startswith('/') or
                link.startswith('https://www.gov.uk/')) and
            # exclude any links containing /government/uploads/system/uploads/attachment_data/file/
            not(link.count('/government/uploads/system/uploads/attachment_data/file/') > 0)]


def extract_links_from_content_details(data):
    """
    Recurses through lists and dicts to find html and then extract links BE
    VERY CAREFUL AND PASS IN LINKS, otherwise old links may persist in the list
    :param data: This function can accept a nested list or dict, or string
    :return:
    """
    if type(data) == list:
        return list(chain.from_iterable([
            extract_links_from_content_details(item)
            for item in data
        ]))
    elif type(data) == dict:
        return extract_links_from_content_details(list(data.values()))
    elif is_html(data):
        return extract_links_from_html(data)
    else:
        return []


def clean_page_path(page_path, sep='#'):
    """
    We want to remove double slashes and replace them with single slashes, and we only want the page_path before a #
    :param page_path:
    :param sep: default '#' that we want to get rid of, we use rsplit to take away the right-most part of the string
    :return: a nice clean page_path, more hope of matching to our content ID mapping
    """
    return page_path.replace('//', '/').rsplit(sep, 1)[0]
