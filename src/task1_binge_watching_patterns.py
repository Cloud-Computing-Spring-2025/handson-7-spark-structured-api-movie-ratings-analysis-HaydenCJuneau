from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, round as spark_round

def initialize_spark(app_name="Task1_Binge_Watching_Patterns"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def detect_binge_watching_patterns(df: DataFrame) -> DataFrame:
    """
    Identify the percentage of users in each age group who binge-watch movies.
    """
    filtered = df.filter((df.IsBingeWatched == True))

    total_counts = df.groupby("AgeGroup").count().withColumnRenamed("count", "total_count")
    binge_counts = filtered.groupby("AgeGroup").count().withColumnRenamed("count", "binge_watchers")

    result = binge_counts.join(total_counts, "AgeGroup")
    result = result.withColumn("percentage", (col("binge_watchers") / col("total_count")) * 100)
    
    return result

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 1.
    """
    spark = initialize_spark()

    input_file = "/opt/bitnami/spark/Movie/input/movie_ratings_data.csv"
    output_file = "/opt/bitnami/spark/Movie/output/binge_watching_patterns.csv"

    df = load_data(spark, input_file)
    result_df = detect_binge_watching_patterns(df)  # Call function here
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
