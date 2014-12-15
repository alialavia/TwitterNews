"""
Generating random subsamples from twitter stream data
"""

import os
import pickle
import json
from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import SVMWithSGD
from preprocessing import is_valid, is_english, parse_json, parse, remove_newlines


def prediction_string(tweet):
    """
    Converts a predicted tweet to a string format
    :param tweet: The tweet
    :type tweet: tuple
    :return: A string representing the predicted tweet
    :rtype: str
    """
    contents = tweet[0]
    labels = tweet[1]
    return ",".join(labels) + " :: " + contents['created_at'][:19] + " :: " + remove_newlines(contents['text'])


def fast_predict(sc, file_input, file_output, sports_model, politics_model, technology_model):
    """
    Predicts using the provided models
    """
    tweets = sc.textFile(file_input).map(parse_json).filter(lambda x: is_valid(x) and is_english(x))
    try:
        print("Reading stored classification model")
        sports = pickle.load(open(sports_model, 'rb'))
        politics = pickle.load(open(politics_model, 'rb'))
        technology = pickle.load(open(technology_model, 'rb'))

        def predict_labels(tweet):
            x = parse(tweet)
            labels = []
            if sports.predict(x) > 0.0:
                labels.append("sports")
            if politics.predict(x) > 0.0:
                labels.append("politics")
            if technology.predict(x):
                labels.append("technology")
            return labels


        print("Computing predictions")
        predictions = tweets.map(lambda t: (t, predict_labels(t)))
        filtered_predictions = predictions.filter(lambda t: len(t[1]) == 1)
        filtered_predictions.map(prediction_string).saveAsTextFile(file_output)
        print("Done!")
    except Exception as e:
        print("Error:")
        print(e)


def main():
    """
    Main entry point of the application
    """

    # Create spark configuration and spark context
    include_path = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'preprocessing.py'))
    conf = SparkConf()
    conf.set('spark.executor.memory', '1500m')
    conf.setAppName("Generating predictions")
    sc = SparkContext(conf=conf, pyFiles=[include_path])

    # Set S3 configuration
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY'])
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_KEY'])

    # Single-pass predictions
    fast_predict(sc, file_input="s3n://twitter-stream-data/twitter-*",
                 file_output="s3n://twitter-stream-predictions/final",
                 sports_model="PyTwitterNews/models/sports.model",
                 politics_model="PyTwitterNews/models/politics.model",
                 technology_model="PyTwitterNews/models/technology.model")

    # Stop application
    sc.stop()


if __name__ == "__main__":
    main()
