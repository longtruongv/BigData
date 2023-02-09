from tqdm import tqdm
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row

# if __name__ == "__main__":

print("#################\n#################\n#################\n#################\n#################\n#################\n#################\n#################\n")

spark = (SparkSession.builder
        .appName("ProcessPlayer")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.5.0")
        .getOrCreate()
    )