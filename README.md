Automatic News Generation Based on Twitter
=======

The system is to train a classifier for several news topics using the news twitter accounts as training data. Then we use this classifier to predict the massive stream of public tweets into these news topics. Any tweets that are not classified as 'sports', 'politics' or 'technology' are discarded. This filters out most of the non-relevant tweets. Finally the predicted tweets are used to compute term frequencies over time and detect interesting news trends.

The system is entirely build using python and we use the NLTK and Scikit-Learn libraries to do the preprocessing. For each tweet we extract the text, tokenize it, stem it and turn it into a sparse feature vector by using the hashing trick. The classifier is trained using Spark's MLLib.

Three components are included in this system:
* Data collector
* Twitter news classifier
* Visualization of news key-terms over time

### Data Collector
Our data collection consists of two parts: 
* Training data, from news agencies, which are collected from their Twitter accounts 
* Unlabeled twitter data is collected over a time span of two months, with a ten-day break in between due to server maintainance. The data is stored as a set of 100,000-tweet files. Later on, we classify this data using our classifier.

In order to run the `data-collectors` script to collect twitter data from [Twitter Streams](https://dev.twitter.com/streaming/public), you need to register your own twitter app key first. The api key should be set in two files: `data-collectors/twitter/twitter.py` and `data-collectors/news/twitter/twitternews.config`. 

For collecting the news twitter data, set the news agent twitter account that you are interest under `data-collectors/news/twitter/twitternews.config`. The config file is in JSON format. Edit the `accounts` section, put the news agent display name under the node `screen_name` and the file path where your data is going to be stored under the node `path`.

Start collecting twitter data by running `python twitter.py`

### Classifier
We configured a Stochastic Gradient Descent (SGD) classifier to classify twitter posts. The classifier should be trained using a set of categorized news. We use a number of news agencies as our source of training data. To train the classifier, test the classifier or run predictions use

    ./bin/pyspark spark-scripts/train.py --py-files spark-scripts/preprocessing.py
    ./bin/pyspark spark-scripts/test.py --py-files spark-scripts/preprocessing.py
    ./bin/pyspark spark-scripts/predict.py --py-files spark-scripts/preprocessing.py

### Visualization analysis
We visualize the classified tweets by finding the top 10 frequent terms per hour and showing them on a [web page](http://aliavi.com/twnews/bigdata/graph/visualization/). 

