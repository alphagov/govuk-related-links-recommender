from datetime import datetime, timedelta


class DateHelper:

    @classmethod
    def get_datetime_for_yesterday(cls, date=None, date_format='%Y%m%d'):
        return cls.get_datetime_for_days_ago(1, date, date_format)

    @classmethod
    def get_datetime_for_weeks_ago(cls, weeks, date=None, date_format='%Y%m%d'):
        return cls.get_datetime_for_days_ago(weeks * 7, date, date_format)

    @classmethod
    def get_datetime_for_days_ago(cls, days, date=None, date_format='%Y%m%d'):
        if date is None:
            date = datetime.today()
        return (date - timedelta(days)).strftime(date_format)
