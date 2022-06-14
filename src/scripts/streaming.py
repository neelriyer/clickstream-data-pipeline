import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from etl import create_spark_session

# spark-submit --master local scripts/streaming.py
# python --master local streaming.py

# TODO: change batch size to 1000 records
def batch_function(df, batch_id):
	min_date, max_date = df.select(F.min("datetime"), F.max("datetime")).first()
	n_unique = df.select('anonymous_user_id').distinct().count()
	return {'min_date': min_date,\
			'max_date': max_date,\
			'n_unqiue': n_unique,\
			'batch_id': batch_id}
	# print("inside foreachBatch for batch_id:{0}, rows in passed dataframe: {1}".format(batch_id, df.count()))

def run_streaming(spark, input_data, output_data):
	path = os.path.join(input_data, 'data.csv')
	df = spark.read.option("header", "true").csv(path)
	
	# lines = spark.readStream.\
	# 		format('csv').\
	# 		option('path', path).\
	# 		schema(df.schema).\
	# 		start()
	
	lines = spark.readStream.schema(df.schema).parquet(os.path.join(output_data, 'clickstream/year=2018/month=4/'))
	
	# lines.createOrReplaceTempView("tempView")
	# result_df = spark.sql('select * from tempView')
	
	save_loc = "/tmp/example"
	query = (lines.writeStream.trigger(once=True)\
			.foreachBatch(batch_function)\
			.option('checkpointLocation', save_loc + "/_checkpoint")\
			.start(save_loc)\
			)
	query.awaitTermination()

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
	run_streaming(spark, input_data, output_data)


if __name__ == "__main__":
	main()
