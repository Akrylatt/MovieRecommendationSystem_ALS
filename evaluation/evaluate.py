from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import MatrixFactorizationModel

from pyspark import SparkContext
sc = SparkContext()

model_path = "/model"
same_model = MatrixFactorizationModel.load(sc, model_path)

complete_ratings_file_eval = "/content/eval/ratings.csv"
raw_eval = sc.textFile(complete_ratings_file_eval)

data_eval = raw_eval.filter(lambda line: line!=complete_ratings_raw_data_header)\
    .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

users_to_recommend = data_eval.map(lambda x: (x[0], x[1]))
new_user_recommendations_RDD = complete_model.predictAll(users_to_recommend)

new_user_recommendations_rating_RDD = new_user_recommendations_RDD.map(lambda x: (x.product, x.rating))
new_user_recommendations_rating_title_and_count_RDD = new_user_recommendations_rating_RDD.join(complete_movies_titles).join(movie_rating_counts_RDD)
new_user_recommendations_rating_title_and_count_RDD.take(3)

new_user_recommendations_rating_title_and_count_RDD = new_user_recommendations_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

"""Getting the recommendations in human-friendly shape"""

top_movies = new_user_recommendations_rating_title_and_count_RDD.filter(lambda r: r[2]>=25).takeOrdered(25, key=lambda x: -x[1])

print ('TOP recommended movies (with more than 25 reviews):\n%s' %
        '\n'.join(map(str, top_movies)))





