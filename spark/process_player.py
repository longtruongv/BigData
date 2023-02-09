from tqdm import tqdm
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row

if __name__ == "__main__":

    # conf = SparkConf()
    # conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.5.0")

    print("#################\n#################\n#################\n#################\n#################\n#################\n#################\n#################\n")

    spark = (SparkSession.builder
        .appName("ProcessPlayer")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.5.0")
        .getOrCreate()
    )

    player_df = (spark.read
        .format("com.mongodb.spark.sql.DefaultSource")
        .option('uri', "mongodb://mongodb:27017/football_data_new.player")
        .load()
    )

    player_df.persist()

    
    info_table_schema = player_df.select('info').schema[0].dataType
    # info_table = spark.createDataFrame(
    #     data = [],
    #     schema = info_table_schema
    # )
    info_table = None
    info_table_flag = False

    stats_table_schemas = {}
    stats_tables = {}
    stats_table_names = player_df.schema['stats'].dataType.fieldNames()
    for table_name in stats_table_names:
        full_table_name = 'stats.' + table_name
        table_schema = player_df.select(full_table_name).schema[0].dataType.elementType
        stats_table_schemas[table_name] = table_schema


    for player_row in tqdm(player_df.collect()[:20]):
        player_id = player_row['_id']
        player_info = player_row['info']
        player_stats = player_row['stats']

        try:
            info_df = spark.createDataFrame([player_info], schema=info_table_schema) 
            info_df = info_df.withColumn("PlayerID", lit(player_id))
            if not info_table_flag:
                info_table = info_df
                info_table.persist()
                info_table_flag = True
            else:
                info_table = info_table.union(info_df)
        except Exception as e: 
            print(e)
            print("ERROR when parsing info", player_id, player_info)

        for table_name in stats_table_names:
            table_data = player_stats[table_name]
            if not table_data:
                continue

            table_schema = stats_table_schemas[table_name]
            try:
                sub_stats_df = spark.createDataFrame(table_data, schema=table_schema) 
                sub_stats_df = sub_stats_df.withColumn("PlayerID", lit(player_id))
                sub_stats_df = sub_stats_df.withColumn("PlayerName", lit(player_info['ShortName']))

                if table_name in stats_tables:
                    stats_tables[table_name] = stats_tables[table_name].union(sub_stats_df)
                else:
                    stats_tables[table_name] = sub_stats_df    
                    stats_tables[table_name].persist()
            except Exception as e: 
                print(e)
                print("ERROR when parsing stats", player_id, table_name)

    player_df.unpersist()

    (info_table.write.format("jdbc")
        .mode('overwrite')
        # .mode('append')
        .option("url", "jdbc:postgresql://postgres:5432/news_crawled") # news_crawled
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "player_info")
        .option("user", "test1")
        .option("password", "test1")
        .save()
    )

    for table_name in stats_tables:
        full_table_name = 'player_' + table_name
        table_df = stats_tables[table_name]
        
        (table_df.write.format("jdbc")
            # .mode('append')
            .mode('overwrite')
            .option("url", "jdbc:postgresql://postgres:5432/news_crawled")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", full_table_name)
            .option("user", "test1")
            .option("password", "test1")
            .save()
        )

    spark.stop()