import unittest
import tap_taboola
import pytz

from datetime import datetime, timedelta


class TestDateRangeUtility(unittest.TestCase):

    def test_daterange_normal(self):
        """
        When given two dates 7 days apart, function should return
        generator that iterates 8 sets of tuples where the second
        value equals the next day's first.

        The last iteration should be the same as
        (end_date, end_date + timedelta(1)), where the time portion
        of the date has been set to 0:00.
        """

        start_date = datetime(2018, 1, 1)
        end_date = start_date + timedelta(7)

        self.assertEqual(
            list(tap_taboola.fetch_campaigns('fake-access-token', 123))        )