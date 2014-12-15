"""
Generating random subsamples from twitter stream data
"""

import os
import pickle
from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import SVMWithSGD
from preprocessing import is_valid, is_english, parse_json, parse_positive, parse_negative


def train(sc, file_positive, files_negative, file_output):
    """
    Trains a binary classification model using positive samples in file_positive and
    negative samples in file_negative. It writes the resulting model to file_output

    :param sc: The spark context
    :type sc: SparkContext
    :param file_positive: The file with positive tweets (relevant ones)
    :type file_positive: str
    :param files_negative: The file with negative tweets (non-relevant ones)
    :type files_negative: list[str]
    :param file_output: The output where to store the trained model
    :type file_output: str
    """
    positive_tweets = sc.textFile(file_positive).map(parse_json).filter(is_valid)
    negative_tweets = [sc.textFile(file_negative).map(parse_json).filter(is_valid) for file_negative in files_negative]
    positive = positive_tweets.map(parse_positive)
    negatives = [nt.map(parse_negative) for nt in negative_tweets]
    data = positive
    for negative in negatives:
        data = data.union(negative)

    try:
        print("Training classification model")
        model = SVMWithSGD.train(data, iterations=150, step=1000.0, regType='l1', regParam=1e-7)
        print("Saving classification model to file")
        pickle.dump(model, open(file_output, 'wb'))
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
    conf.setAppName("Training classifier")
    sc = SparkContext(conf=conf, pyFiles=[include_path])

    # Set S3 configuration
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY'])
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_KEY'])

    # Train model for sports
    train(sc, file_positive="s3n://twitter-training-data/sports/twitter-*",
          #file_negative="s3n://twitter-training-data/other/twitter-*",
          files_negative=["s3n://twitter-training-data/other/twitter-*",
                          "s3n://twitter-training-data/politics/twitter-*",
                          "s3n://twitter-training-data/technology/twitter-*"],
          file_output="PyTwitterNews/models/sports.model")

    # Train model for politics
    train(sc, file_positive="s3n://twitter-training-data/politics/twitter-*",
          files_negative=["s3n://twitter-training-data/other/twitter-*",
                          "s3n://twitter-training-data/sports/twitter-*",
                          "s3n://twitter-training-data/technology/twitter-*"],
          file_output="PyTwitterNews/models/politics.model")

    # Train model for technology
    train(sc, file_positive="s3n://twitter-training-data/technology/twitter-*",
          files_negative=["s3n://twitter-training-data/other/twitter-*",
                          "s3n://twitter-training-data/sports/twitter-*",
                          "s3n://twitter-training-data/politics/twitter-*"],
          file_output="PyTwitterNews/models/technology.model")

    # Stop application
    sc.stop()


if __name__ == "__main__":
    main()
