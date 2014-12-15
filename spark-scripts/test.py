"""
Generating random subsamples from twitter stream data
"""

import os
import pickle
from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import SVMWithSGD

from preprocessing import is_valid, is_english, parse_json, parse


def test(sc, file_positive, files_negative, file_model):
    """
    Tests a classification model using positive samples in file_positive and
    negative samples in file_negative. It prints the results to standard output

    :param sc: The spark context
    :type sc: SparkContext
    :param file_positive: The file with tweets to predict
    :type file_positive: str
    :param files_negative: The files with tweets to reject
    :type files_negative: list[str]
    :param file_model: The file where the model is located
    :type file_model: str
    """
    tweets_positive = sc.textFile(file_positive).map(parse_json).filter(lambda x: is_valid(x) and is_english(x)).cache()
    list_negatives = [sc.textFile(file_negative).map(parse_json).filter(lambda x: is_valid(x) and is_english(x)) for file_negative in files_negative]
    tweets_negative = list_negatives[0]
    for ln in list_negatives[1:]:
        tweets_negative = tweets_negative.union(ln)
    try:
        print("Reading stored classification model")
        model = pickle.load(open(file_model, 'rb'))
        print("Computing predictions")
        threshold = 0.0
        total_positive = tweets_positive.count()
        total_negative = tweets_negative.count()
        true_positives = tweets_positive.filter(lambda x: model.predict(parse(x)) > threshold).count()
        true_negatives = tweets_negative.filter(lambda x: model.predict(parse(x)) <= threshold).count()
        false_negatives = total_positive - true_positives
        false_positives = total_negative - true_negatives
        print("Results for %s:" % file_model)
        print("  Total positives: %d" % total_positive)
        print("  Total negatives: %d" % total_negative)
        print("  False positives: %d" % false_positives)
        print("  False negatives: %d" % false_negatives)
        precision = 0.0
        recall = 0.0
        try:
            precision = float(true_positives) / float(true_positives + false_positives)
            recall = float(true_positives) / float(true_positives + false_negatives)
        except:
            pass
        print("  Precision: %f" % precision)
        print("  Recall: %f" % recall)
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
    conf.setAppName("Testing classifier")
    sc = SparkContext(conf=conf, pyFiles=[include_path])

    # Set S3 configuration
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY'])
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_KEY'])

    # Test model for sports
    test(sc, file_positive="s3n://twitter-training-data/sports/test-*",
         files_negative=["s3n://twitter-test-data/other/twitter-*",
                         "s3n://twitter-training-data/politics/test-*",
                         "s3n://twitter-training-data/technology/test-*"],
         file_model="PyTwitterNews/models/sports.model")

    # Test model for politics
    test(sc, file_positive="s3n://twitter-training-data/politics/test-*",
         files_negative=["s3n://twitter-test-data/other/twitter-*",
                         "s3n://twitter-training-data/sports/test-*",
                         "s3n://twitter-training-data/technology/test-*"],
         file_model="PyTwitterNews/models/politics.model")

    # Test model for technology
    test(sc, file_positive="s3n://twitter-training-data/technology/test-*",
         files_negative=["s3n://twitter-test-data/other/twitter-*",
                         "s3n://twitter-training-data/politics/test-*",
                         "s3n://twitter-training-data/sports/test-*"],
         file_model="PyTwitterNews/models/technology.model")

    # Stop application
    sc.stop()


if __name__ == "__main__":
    main()
