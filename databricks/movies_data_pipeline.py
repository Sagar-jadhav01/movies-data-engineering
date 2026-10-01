from pyspark import pipelines as dp


@dp.table
def bronze_movies():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .option("sep", ";")
        .load("s3://sagar-movies-data-project/raw/")
    )


@dp.table
def silver_movies():

    from pyspark.sql.functions import col

    return (
        spark.readStream
        .table("bronze_movies")
        .dropDuplicates(["name", "year"])
        .filter(
            col("name").isNotNull() &
            col("year").isNotNull() &
            col("score").isNotNull()
        )
    )



@dp.table
def gold_genre_analytics():

    from pyspark.sql.functions import count, avg, sum

    return (
        spark.read.table("silver_movies")
        .groupBy("genre")
        .agg(
            count("*").alias("total_movies"),
            avg("score").alias("average_score"),
            sum("gross").alias("total_gross")
        )
    )