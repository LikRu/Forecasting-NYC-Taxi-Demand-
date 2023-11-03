from urllib.request import urlretrieve
import os
import zipfile

# data output directory is `data/landing/`
output_relative_dir = 'data/landing'

# check if it exists as it makedir will raise an error if it does exist
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)

# now, we will create the path for the taxi_zones data
for target_dir in ('/taxi_zones_data', '/weather_data'):  # taxi_zones should already exist
    if not os.path.exists(output_relative_dir + target_dir):
        os.makedirs(output_relative_dir + target_dir)

# Adjust the range function from jan to may for year 2023
YEAR = '2023'
MONTHS = range(1, 6)
# link for yellow and green taxis data
URL_YELLOW = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
URL_GREEN = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"


for month in MONTHS:
    # 0-fill i.e 1 -> 01, 2 -> 02, etc
    month = str(month).zfill(2)
    print(f"Begin month {month}")

    # generate url
    url_yellow = f'{URL_YELLOW}{YEAR}-{month}.parquet'
    url_green = f'{URL_GREEN}{YEAR}-{month}.parquet'
    # generate output location and filename
    output_dir_yellow = f"{output_relative_dir}/{YEAR}-{month}-yellow.parquet"
    output_dir_green = f"{output_relative_dir}/{YEAR}-{month}-green.parquet"
    # download
    urlretrieve(url_yellow, output_dir_yellow)
    urlretrieve(url_green, output_dir_green)

    print(f"Completed month {month}")

# download the taxis data for november and december of 2022
for month in [11, 12]:
    url_yellow = f'{URL_YELLOW}2022-{month}.parquet'
    url_green = f'{URL_GREEN}2022-{month}.parquet'
    # generate output location and filename
    output_dir_yellow = f"{output_relative_dir}/2022-{month}-yellow.parquet"
    output_dir_green = f"{output_relative_dir}/2022-{month}-green.parquet"
    # download
    urlretrieve(url_yellow, output_dir_yellow)
    urlretrieve(url_green, output_dir_green)

# download the weather data for year 2022 and 2023
output_dir_weather = output_relative_dir + "/weather_data/weather-2022.csv"
input_weather = "https://www.ncei.noaa.gov/data/global-hourly/access/2022/72503014732.csv"
urlretrieve(input_weather, output_dir_weather)
urlretrieve("https://www.ncei.noaa.gov/data/global-hourly/access/2023/72503014732.csv",
            output_relative_dir + "/weather_data/weather-2023.csv")

# download the taxi_zones data
urlretrieve("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
            f"{output_relative_dir}/taxi_zones_data/taxi_zones.zip")
urlretrieve("https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv",
            f"{output_relative_dir}/taxi_zones_data/taxi_zone_lookup.csv")


def unzip_file(zip_filepath, output_directory):
    """
    Unzip a file and save it to the output directory.

    Parameters:
    - zip_filepath: Path to the zip file.
    - output_directory: Directory where the unzipped files will be stored.
    """

    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(output_directory)


# unzip the taxi_zones data and store it in the taxi_zones folder
unzip_file('data/landing/taxi_zones_data/taxi_zones.zip',
           'data/landing/taxi_zones_data')
