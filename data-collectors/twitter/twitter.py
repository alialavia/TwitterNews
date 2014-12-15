from requests_oauthlib import OAuth1
from time import sleep, strftime
import requests
import logging

TIMEOUT = 60
MAX_TWEETS_IN_FILE = 100000
APP_KEY = "your-twitter-app-key"
APP_SECRET = "your-twitter-app-secret"
OAUTH_TOKEN = "your-twitter-oauth-token"
OAUTH_TOKEN_SECRET = "your-twitter-oauth-secret"


class TwitterStream(object):
    def __init__(self, consumer_key=None, consumer_secret=None, access_token_key=None, access_token_secret=None):
        self.auth = OAuth1(consumer_key, consumer_secret, access_token_key, access_token_secret)

    def request_tweets(self, params=None):
        session = requests.Session()
        session.auth = self.auth
        session.stream = True
        timeout = 90 # Twitter recommendation for streaming API
        url = "https://stream.twitter.com/1.1/statuses/filter.json"
        self.response = session.request("POST", url, data=params, params=params, timeout=timeout)

    def __iter__(self):
        for item in self.response.iter_lines(chunk_size=512):
            if item:
                yield item


def get_new_filename(basename=""):
    return "%s%s" % (basename, strftime("%Y%m%d%H%M%S"))


def main():
    logger = logging.getLogger("twitter")
    loghandler = logging.FileHandler("twitter.log")
    loghandler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(loghandler)
    logger.setLevel(logging.INFO)

    twitter_stream = TwitterStream(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    while True:
        output_stream = None
        try:
            
            twitter_stream.request_tweets({'locations':'-180,-90,180,90', 'language':'en'})
            logger.info("Requested twitter stream")
            
            tweet_count = 0
            output_stream = file(get_new_filename("twitter-"), "w")
            logger.info("Opened output file %s" % output_stream.name)

            logger.info("Writing results")
            for item in twitter_stream:
                output_stream.write(item)
                output_stream.write("\n")
                tweet_count += 1

                # If we reach the maximum number of tweets, we move to a new file
                if tweet_count >= MAX_TWEETS_IN_FILE:
                    output_stream.close()
                    output_stream = file(get_new_filename("twitter-"), "w")
                    logger.info("Opened next file: %s" % output_stream.name)
                    tweet_count = 0

        except Exception as e:
            logger.error(e.message)

        finally:
            if output_stream is not None and not output_stream.closed:
                output_stream.close()
            logger.info("Retrying connection in %d seconds" % TIMEOUT)
            sleep(TIMEOUT)


if __name__ == "__main__":
    main()

