from unittest.mock import Mock
from requests.exceptions import Timeout
from weather_homework.ingest import get_source_data
import pytest

def test_get_source_data_timeout(mocker):
    mock_r=Mock()
    mocker.patch('weather_homework.ingest.requests',mock_r)
    mock_r.get.side_effect=Timeout
    with pytest.raises(SystemExit):
        get_source_data("")

def test_get_source_data_wrong_code(mocker):
    mock_r=Mock()
    mocker.patch('weather_homework.ingest.requests',mock_r)
    mock_r.get.status_code=300
    with pytest.raises(SystemExit):
        get_source_data("")

def test_get_source_ok():
    r=get_source_data("https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_2013_2017.txt")
    assert r.text.split('\n')[0] == '2013  1  1  988.1  991.1  993.9'