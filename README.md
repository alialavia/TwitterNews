Automatic News Generation Based on Twitter
=======

The system is to train a classifier for several news topics using the news twitter accounts as training data. Then we use this classifier to predict the massive stream of public tweets into these news topics. Any tweets that are not classified as 'sports', 'politics' or 'technology' are discarded. This filters out most of the non-relevant tweets. Finally the predicted tweets are used to compute term frequencies over time and detect interesting news trends.

The system is entirely build using python and we use the NLTK and Scikit-Learn libraries to do the preprocessing. For each tweet we extract the text, tokenize it, stem it and turn it into a sparse feature vector by using the hashing trick. The classifier is trained using Spark's MLLib.

Three components are included in this system:
* Data collector
* Twitter news classifier
* Visualization of news key-terms over time

### Data Collector
In order to run the `data-collectors` scritp to collect twitter data from [Twitter Streams](https://dev.twitter.com/streaming/public), you need to register your own twitter app key first. The api key should be set in two files: `data-collectors/twitter/twitter.py` and `data-collectors/news/twitter/twitternews.config`. 

For collecting the news twitter data, set the news agent twitter account that you are interest under `data-collectors/news/twitter/twitternews.config`. The config file is in JSON format. Edit the `accounts` section, put the news agent display name under the node `screen_name` and the file path where your data is going to be stored under the node `path`.

Start collecting twitter data by running `python twitter.py`

### Classifier


### Visualization analysis


