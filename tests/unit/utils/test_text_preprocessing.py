import pytest

from src.utils.text_preprocessing import *


def test_is_html():
    assert is_html('<h1>this is a heading</h1>') is True
    assert is_html(3) is None
    assert is_html('this is not html') is False
