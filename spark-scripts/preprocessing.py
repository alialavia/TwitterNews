from scipy.sparse import csr_matrix, vstack
from sklearn.feature_extraction.text import HashingVectorizer
from nltk import word_tokenize
from nltk.stem import LancasterStemmer
from pyspark.mllib.regression import LabeledPoint
import json
import re


class Tokenizer():
    """
    Tokenizes and stems text using NLTK libraries
    """

    def __init__(self):
        """
        Constructs a tokenizer object
        """
        self.stemmer = LancasterStemmer()

    def __call__(self, text):
        """
        Tokenizes text

        :param text: the text to tokenize
        :type text: str or unicode
        :return: a list of tokens
        :rtype: list of (str or unicode)
        """
        return [self.stemmer.stem(token) for token in word_tokenize(text)]


# Construct a hashing vectorizer
hash_vectorizer = HashingVectorizer(n_features=2**20,
                       tokenizer=Tokenizer(),
                       lowercase=True,
                       strip_accents='unicode',
                       stop_words='english',
                       ngram_range=(1, 3))


def remove_urls(text):
    """
    Removes urls from given text
    :param text: The text to remove the urls from
    :type text: str
    :return: The text without the urls
    :rtype: str
    """
    return re.sub(r'https?:\/\/[^\t ]*', '', text)


def remove_newlines(text):
    """
    Removes new lines from given text
    :param text: The text to remove the new lines from
    :type text: str
    :return: The text without the new lines
    :rtype: str
    """
    return text.replace('\n', ' ').replace('\r', '')


def parse_positive(tweet):
    """
    Parses a relevant tweet
    :param tweet: The tweet
    :type tweet: dict
    :return: The labeled point for use in MLLib classifiers
    :rtype: LabeledPoint
    """
    return parse_labeled(tweet, 1.0)


def parse_negative(tweet):
    """
    Parses a non-relevant tweet
    :param tweet: The tweet
    :type tweet: dict
    :return: The labeled point for use in MLLib classifiers
    :rtype: LabeledPoint
    """
    return parse_labeled(tweet, 0.0)


def parse_labeled(tweet, label):
    """
    Parses a tweet into an MLLib labeled point
    :param tweet: The tweet
    :type tweet: dict
    :param label: The corresponding label
    :type label: float
    :return: The labeled point for use in MLLib classifiers
    :rtype: LabeledPoint
    """
    vec = parse(tweet)
    return LabeledPoint(label, vec) # Create MLLib LabeledPoint


def parse(tweet):
    """
    Parses a tweet
    :param tweet: The tweet to parse
    :type tweet: dict
    :return: The sparse csr_matrix representing the tweet
    :rtype: csr_matrix
    """
    text = tweet['text'] # Extract text from JSON
    text = remove_newlines(text)
    text = remove_urls(text)
    vec = hash_vectorizer.transform([text]).T # Use hashed text features
    return vec # Return sparse vector


def parse_json(raw_tweet):
    """
    Converts given raw tweet to a dictionary representing the json structure
    :param raw_tweet: The tweet
    :type raw_tweet: str
    :return: A dictionary representing the json structure
    :rtype: dict
    """
    try:
        return json.loads(raw_tweet)
    except Exception:
        return {}


def is_valid(tweet):
    """
    Checks if given tweet is valid by the existence of the 'text' field
    :param tweet: The tweet
    :type tweet: dict
    :return: True if the tweet is valid, false otherwise
    :rtype: bool
    """
    return tweet is not None and 'text' in tweet


def is_english(tweet):
    """
    Checks if given tweet is valid by the existence of the 'text' field
    :param tweet: The tweet
    :type tweet: dict
    :return: True if the tweet is valid, false otherwise
    :rtype: bool
    """
    return 'lang' in tweet and tweet['lang'] == 'en'

