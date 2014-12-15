from requests_oauthlib import OAuth1
from time import sleep
from StringIO import StringIO
import requests
import logging
import json
import os

CONFIG_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "twitternews.config")
CONFIG = json.load(file(CONFIG_FILE, 'r'))


class TwitterNews(object):
    def __init__(self, consumer_key=None, consumer_secret=None, access_token_key=None, access_token_secret=None):
        self.auth = OAuth1(consumer_key, consumer_secret, access_token_key, access_token_secret)

    def request_tweets(self, params=None):                
        url = "https://api.twitter.com/1.1/statuses/user_timeline.json"
        self.response = requests.get(url, params=params, auth=self.auth, timeout=CONFIG["timeout"])


def main():
    logger = logging.getLogger("twitternews")
    loghandler = logging.FileHandler("twitternews.log")
    loghandler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(loghandler)
    logger.setLevel(logging.INFO)


    twitter_news = TwitterNews(CONFIG["app_key"], CONFIG["app_secret"], CONFIG["oauth_token"],
                               CONFIG["oauth_token_secret"])

    # Initialize the previous id to 0
    last_id = {account["screen_name"]: 0 for account in CONFIG["accounts"]}

    # Perform read loop
    while True:
        try:
            for account in CONFIG["accounts"]:
                screen_name = account["screen_name"]
                file_name = account["file"]

                logger.info ("Requesting tweets from @%s after id %d" % (screen_name, last_id[screen_name]))
                params = {'screen_name':screen_name, 'exclude_replies':'true', 'count':'200'}
                if last_id[screen_name] > 0:
                    params['max_id'] = last_id[screen_name]
                twitter_news.request_tweets(params)
                posts = json.load(StringIO(twitter_news.response.content))

                # if there is any error with the response, log it and try next news_source
                if twitter_news.response.status_code != 200:
                    logger.error( "Error Code: %d, %s" % (twitter_news.response.status_code, twitter_news.response.content))
                    continue

                # append tweets to file
                with file(file_name, "a+") as output_stream:
                    logger.info("Opened output file %s" % output_stream.name)
                    for item in posts:
                        output_stream.write(json.dumps(item))
                        output_stream.write("\n")
                        if last_id[screen_name] == 0:
                            last_id[screen_name] = item['id']-1
                        else:
                            last_id[screen_name] = min(last_id[screen_name], item['id']-1)

                sleep(CONFIG["request_delay"])

        except Exception as e:
            logger.error(e.message)


if __name__ == "__main__":
    main()

