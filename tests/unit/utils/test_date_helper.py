import pytest
import datetime

from src.utils.date_helper import DateHelper

def test_get_datetime_for_yesterday_should_return_previous_day():
    today_datetime = datetime.datetime(2019, 7, 19)
    expected_yesterday_datetime_str = "20190718"

    actual_yesterday = DateHelper.get_datetime_for_yesterday(date=today_datetime)

    assert expected_yesterday_datetime_str == actual_yesterday

def test_get_datetime_for_weeks_ago():
    today_datetime = datetime.datetime(2019, 7, 19)
    expected_two_weeks_ago_datetime_str = "20190705"

    actual_two_weeks_ago = DateHelper.get_datetime_for_weeks_ago(2, date=today_datetime)

    assert expected_two_weeks_ago_datetime_str == actual_two_weeks_ago

def test_get_datetime_for_days_ago():
    today_datetime = datetime.datetime(2019, 7, 19)
    expected_days_ago_datetime_str = "20190715"

    actual_four_days_ago = DateHelper.get_datetime_for_days_ago(4, date=today_datetime)

    assert expected_days_ago_datetime_str == actual_four_days_ago
