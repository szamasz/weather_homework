import requests
from pathlib import Path

TEMPERATURE_SOURCES={
    "1756_1858":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/temperature/daily/"
                "raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt"),
    "1859_1960":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/temperature/"
                "daily/raw/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt"),
    "1961_2012":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/temperature/"
                "daily/raw/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt"),
    "2013_2017_manual":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/temperature/"
                "daily/raw/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt"),
    "2013_2017_automatic":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/temperature/daily/"
                "raw/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
    }
PRESSURE_SOURCES={
    "1753_1858":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/air_pressure/raw/"
                "stockholm_barometer_1756_1858.txt"),
    "1859_1861":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/air_pressure/raw/"
                "stockholm_barometer_1859_1861.txt"),
    "1862_1937":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/air_pressure/raw/"
                "stockholm_barometer_1862_1937.txt"),
    "1938_1960":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/air_pressure/raw/"
                "stockholm_barometer_1938_1960.txt"),
    "1961_2012":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/air_pressure/raw/"
                "stockholm_barometer_1961_2012.txt"),
    "2013_2017_manual":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/air_pressure/raw/"
                "stockholm_barometer_2013_2017.txt"),
    "2013_2017_automatic":("https://bolin.su.se/data/stockholm-thematic/files/"
                "stockholm-historical-weather-observations-2017/air_pressure/raw/"
                "stockholmA_barometer_2013_2017.txt")
}

RAW_DIR="hdfs/raw"
CLEAN_DIR="hdfs/clean"

def create_dirs():
    """
    Creates directories used for storing output files
    Args:

    Returns:

    Raises:
        SystemExit: if creation failes
    """
    try:
        Path(RAW_DIR).mkdir(parents=True,exist_ok=True)
        Path(CLEAN_DIR).mkdir(parents=True,exist_ok=True)
    except Exception as e:
        raise SystemExit(f"failed creation of directories: {e}")

def get_source_data(url):
    """
    Downolads data from the provided URL
    Args:
        url(string): url of file to be downloaded
    
    Returns:
        r(requests object)
    
    Raises:
        SystemExit in case download failes
    """
    try:
        print(url)
        r=requests.get(url)
        if r.status_code != 200 or not r.text:
            raise Exception(f"Download of file failed, r.status_code={r.status_code}")
    except Exception as e:
        raise SystemExit(f"Downloading file {url} failed with error {e}")
    return r

def write_source_data():
    """
    Downloads and writes required failes to directories

    Args:

    Returns:

    Raises:
        SystemExit in case writing failes
    """
    create_dirs()
    for k,v in TEMPERATURE_SOURCES.items():
        r=get_source_data(v)
        with open(f"{RAW_DIR}/{k}_temp.txt","w") as f:
            f.write(r.text)
    
    for k,v in PRESSURE_SOURCES.items():
        r=get_source_data(v)
        with open(f"{RAW_DIR}/{k}_press.txt","w") as f:
            f.write(r.text)

write_source_data()
