import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from urllib.parse import urlparse, parse_qs
from pyspark.sql.types import MapType, StringType


def create_spark_session():
	spark = SparkSession \
		.builder \
		.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
		.getOrCreate()
	return spark

def process_clickstream_data(spark, input_data, output_data):

	path = os.path.join(input_data, 'data.csv')

	df = spark.read.option("header", "true").csv(path)
	# print(df.show(3))

	# extract year and month
	df = df.withColumn('time', F.col('time').cast('int'))
	get_datetime = F.udf(lambda ts: datetime.fromtimestamp(ts), DateType())
	df = df.withColumn('datetime', get_datetime(df.time))
	df = df.withColumn('month', F.month('datetime')).withColumn('year', F.year('datetime'))

	# extract params required
	extract_params = F.udf(lambda x: {k: v[0] for k, v in parse_qs(urlparse(x).query).items()}, MapType(StringType(), StringType()))

	df2 = df.withColumn(
		"params", extract_params(F.col('url'))
	)
	df2 = df2.withColumn(
		"utm_source", df2.params['utm_source']
	).withColumn(
		"utm_medium", df2.params['utm_medium']
	).withColumn(
		"path", df2.params['path']
	).drop("params")

	# df2.filter(F.col('path').isNotNull()).select('path').show(10)
	# df2.filter(F.col('path').isNotNull()).select('path').collect()[0]

	df2.write.partitionBy(['year', 'month']).parquet(os.path.join(output_data, 'clickstream'), 'overwrite')


def main():
		
	config = configparser.ConfigParser()
	config.read('../dl.cfg')

	# # aws cluster mode
	# os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
	# os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

	# input_data = config['DATALAKE']['INPUT_DATA']
	# output_data = config['DATALAKE']['OUTPUT_DATA']

	# local mode
	input_data = config['LOCAL']['INPUT_DATA']
	output_data = config['LOCAL']['OUTPUT_DATA']

	spark = create_spark_session()

	process_clickstream_data(spark, input_data, output_data)


if __name__ == "__main__":
	main()
