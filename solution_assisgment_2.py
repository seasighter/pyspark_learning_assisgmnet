from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import warnings
warnings.filterwarnings("ignore")

'''Write spark job to solve below mentioned problem statements
a. Show the aggregated number of ratings per year
b. Show the average monthly number of ratings
c. Show the rating levels distribution'''


spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

movies_data = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/home/deepakkushwaha6392/pyspark_folder/Class_Assignments/Spark Assignment 2/SparkMovieRatingProject/SparkMovieRatingProject/movies.csv")

rating_data = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferschema", "true")\
    .load("/home/deepakkushwaha6392/pyspark_folder/Class_Assignments/Spark Assignment 2/SparkMovieRatingProject/SparkMovieRatingProject/ratings.csv")
# data = rating_data.withColumn("date", year(from_unixtime(col("timestamp"))))

# Show the aggregated number of ratings per year


def average_rating(rating_data):
    data = rating_data.withColumn(
        "date", year(from_unixtime(col("timestamp"))))
    average_data = data.groupBy("date").agg(count(col("rating")).alias("rating_per_year"))\
        .select("date", "rating_per_year").sort("date")
    average_data.show()


# average_rating(rating_data)

# b. Show the average monthly number of ratings

def average_monthly_rating(rating__data):
    data = rating_data.withColumn(
        "month", month(from_unixtime(col("timestamp"))))
    avg_rating = data.groupBy("month").agg(round(avg(col("rating"))).alias("rating_per_year"))\
        .select("month", "rating_per_year").sort("month")

    avg_rating.show()


# average_monthly_rating(rating_data)

# c. Show the rating levels distribution'''


def rating_distribution(rating_data):
    total_count = rating_data.count()
    distr = rating_data.groupBy("rating").agg(round(
        (count(col("rating"))/total_count)*100)).alias("rating_distribution")
    distr.show()


# rating_distribution(rating_data)

''' d. Show the 18 movies that are tagged but not rated
e. Show the movies that have rating but no tag
f. Focusing on the rated untagged movies with more than 30 user ratings,
show the top 10 movies in terms of average rating and number of
ratings
g. What is the average number of tags per movie in tagsDF? And the
average number of tags per user? How does it compare with the
average number of tags a user assigns to a movie?
 '''
# solution for Q ->> d
tag_data = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferschema", "true")\
    .load("/home/deepakkushwaha6392/pyspark_folder/Class_Assignments/Spark Assignment 2/SparkMovieRatingProject/SparkMovieRatingProject/tags.csv")

# tag_data.show()
unrated_tagged_movie = tag_data.join(
    rating_data, tag_data["movieId"] == rating_data["movieId"], "left").filter(rating_data["movieId"].isNull())\
    .select(tag_data["userId"], tag_data["movieId"], tag_data["tag"], tag_data["timestamp"], rating_data["movieId"])


# unrated_tagged_movie.distinct().show(18)

'''f. Focusing on the rated untagged movies with more than 30 user ratings,
show the top 10 movies in terms of average rating and number of
ratings'''

untagged_rated_movie = rating_data.join(
    tag_data, rating_data["movieId"] == tag_data["movieId"], "left").filter(tag_data["movieId"].isNull())\
    .select(rating_data["userId"], rating_data["movieId"], rating_data["rating"], rating_data["timestamp"])

untagged_rated_movie = untagged_rated_movie.groupBy("movieId").agg(round(avg(col("rating")))
                                                                   .alias("avg_rating"), count(col("rating")).alias("no_of_rating")).select("movieId", "avg_rating", "no_of_rating").orderBy(col("avg_rating").desc(), col("no_of_rating").desc())

untagged_rated_movie.show(10)
