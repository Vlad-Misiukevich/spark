import logging
import os.path
import sys
from typing import Tuple, Union

import pygeohash
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# from config import GEOCODER
from src.main.python.config import GEOCODER


def get_spark_obj() -> SparkSession:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    app_name = "DataExtract"
    master = "local[*]"
    spark_conf = SparkConf() \
        .setAppName(app_name) \
        .setMaster(master) \
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.2.2') \
        .set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "*") \
        .set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
             "*") \
        .set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
             "*") \
        .set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
             "*") \
        .set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
             "*") \
        .set("spark.executor.heartbeatInterval", "110s")

    return SparkSession.builder.config(conf=spark_conf).getOrCreate()


def save_dataframe_to_adls(spark_session: SparkSession, df: DataFrame) -> None:
    spark_session.conf.set(
        "fs.azure.account.key.stvmisiukevich.dfs.core.windows.net",
        "*"
    )
    df.write.format("parquet").save("abfss://data@stvmisiukevich.dfs.core.windows.net/spark_hw")


def get_dfs(spark_obj: SparkSession) -> Tuple[DataFrame, DataFrame]:
    base_path = "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net"
    hotels = spark_obj.read.option("header", "true").csv(base_path + "/hotels")
    weather = spark_obj.read.option("header", "true").parquet(base_path + "/weather")
    return hotels, weather


@udf(returnType=StringType())
def get_latitude(country: str, city: str, address: str) -> Union[float, str]:
    try:
        result = GEOCODER.geocode(f'{country}, {city}, {address}')[0]
    except IndexError:
        return 1.0
    lat = result['geometry']['lat']
    return lat


@udf(returnType=StringType())
def get_longitude(country: str, city: str, address: str) -> Union[float, str]:
    try:
        result = GEOCODER.geocode(f'{country}, {city}, {address}')[0]
    except IndexError:
        return 1.0
    lng = result['geometry']['lng']
    return lng


@udf(returnType=StringType())
def generate_hash_by_lat_lng(lat: str, lng: str) -> Union[Column, None]:
    try:
        return pygeohash.encode(latitude=float(lat), longitude=float(lng), precision=4)
    except Exception:
        return None


if __name__ == '__main__':
    logger = logging.getLogger('py4j')
    spark = get_spark_obj()
    df_hotels, df_weather = get_dfs(spark)
    hotels_with_not_null_lat = df_hotels[df_hotels["Latitude"].isNotNull()]
    hotels_with_null_lat = df_hotels[df_hotels["Latitude"].isNull()]
    updated_hotels_with_null_lat_lng = hotels_with_null_lat.withColumn(
        "Latitude_new",
        get_latitude(
            hotels_with_null_lat["Country"],
            hotels_with_null_lat["City"],
            hotels_with_null_lat["Address"]
        )
    ).withColumn(
        "Longitude_new",
        get_longitude(
            hotels_with_null_lat["Country"],
            hotels_with_null_lat["City"],
            hotels_with_null_lat["Address"]
        )
    ).drop('Latitude', 'Longitude')

    hotels_union = hotels_with_not_null_lat.union(updated_hotels_with_null_lat_lng)
    df_hotels_with_geohash = hotels_union.withColumn(
        "Geohash",
        generate_hash_by_lat_lng(
            hotels_union["Latitude"],
            hotels_union["Longitude"],
        )
    )
    logger.info("Dataframe 'hotels with geohash' created")
    df_weather_with_geohash = df_weather.withColumn(
        "Geohash",
        generate_hash_by_lat_lng(
            df_weather["lng"],
            df_weather["lat"]
        )
    )
    logger.info("Dataframe 'weather with geohash' created")
    df_weather_without_duplicates = df_weather_with_geohash.dropDuplicates(['Geohash'])

    df_result = df_hotels_with_geohash.join(
        df_weather_without_duplicates,
        df_hotels_with_geohash.Geohash == df_weather_without_duplicates.Geohash,
        "left"
    ).select(
        df_hotels_with_geohash["Id"],
        df_hotels_with_geohash["Name"],
        df_hotels_with_geohash["Geohash"],
        df_weather_without_duplicates["avg_tmpr_c"],
        df_weather_without_duplicates["wthr_date"]
    )
    logger.info("Dataframe with join tables created")
    save_dataframe_to_adls(spark, df_result)
