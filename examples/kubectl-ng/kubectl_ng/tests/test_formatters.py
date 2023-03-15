from kubectl_ng._formatters import time_delta_to_string


def test_time_delta_to_string():
    from datetime import timedelta

    assert time_delta_to_string(timedelta(seconds=1), 1) == "Just Now"
    assert time_delta_to_string(timedelta(seconds=10), 1) == "10s"
    assert time_delta_to_string(timedelta(minutes=1, seconds=5), 1) == "1m"
    assert time_delta_to_string(timedelta(minutes=1, seconds=5), 1, " ago") == "1m ago"
    assert time_delta_to_string(timedelta(minutes=1, seconds=5), 2) == "1m5s"
    assert time_delta_to_string(timedelta(hours=1), 1) == "1h"
    assert time_delta_to_string(timedelta(hours=3), 1) == "3h"
    assert time_delta_to_string(timedelta(days=3, hours=4, minutes=2), 3) == "3d4h2m"
    assert time_delta_to_string(timedelta(days=3, hours=4, minutes=2), 2) == "3d4h"
