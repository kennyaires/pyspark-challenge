from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("PySpark App").setMaster("local")
sc = SparkContext(conf=conf)

# Upload files into spark context
july_access_log = sc.textFile('access_log_Jul95').cache()