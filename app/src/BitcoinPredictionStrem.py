# -*- coding: utf-8 -*-
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import pprint
import json
import re
import matplotlib.pyplot as plt
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from pyspark.ml.feature import CountVectorizer, StringIndexer
from http import client as httpClient
from http import HTTPStatus
import datetime
import time
from newsapi import NewsApiClient

from pyspark.sql import Row
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.classification import NaiveBayes
from pyspark.mllib.util import MLUtils

from BitcoinPriceIndex_historical import getHistoricalPrice, createHistoricalDataset

access_token = '963341451273887744-fyNcKmcLd2HRYktyU3wVMshB4eYWMoh'
access_token_secret = 'fxOtX3rk3KXqiF50mFDEYTx19E3wNVMZeSIuXmozNxmHa'
consumer_key = 'bG58SBJQV8Hiqqjcu3jzXwfCL'
consumer_secret = 'kjcBffzpn9QYZsV91NZUqKhgGKBvehLyVfuvc0pm8Gh8sEPui8'

def connectionToTwitterAPI():
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return listener, auth

class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream. 
    This is a basic listener that just prints received tweets to stdout.

    """

    def __init__(self):
        self.tweet = ""

    def on_data(self, data):
        self.tweet = json.loads(data)['text']
        return True

    def on_error(self, status):
        print(status)

def getTweets(auth, date_since):
    api = tweepy.API(auth)
    search_terms = "#bitcoin"
    for tweet in tweepy.Cursor(api.search,\
                        q=search_terms,\
                        since=date_since,\
                        lan="en",
                        count='10')\
                        .items():
        current_tweet = {}
        current_tweet['date'] = tweet.created_at
        current_tweet['text'] = tweet.text
        yield current_tweet

DEFAULT_HOST = "newsapi.org"

def getGoogleArticle(date,host=DEFAULT_HOST):
    connection = httpClient.HTTPConnection(host)
    date_start=date+"T00:00:00"
    date_end=date+"T23:59:00"
    uri="/v2/everything?q=bitcoin&from="+date_start+"&to="+date_end+"&pageSize=10&apiKey=3452795ae84242bd87160c899376718a"
    connection.request("GET", uri)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def cleaningText(tweet):
    tweet_lower = tweet.lower()
    tweet_nonAlpha = re.sub('[^a-zA-Z@# ]','',tweet_lower)
    tweet_split = tweet_nonAlpha.split(' ')
    tweet_not_doublons = set(tweet_split)
    tweet_clean = []
    stop_words = set(stopwords.words('english'))
    for word in tweet_not_doublons:
        if word not in stop_words:
            tweet_clean.append(word)
    return tweet_clean

def getVADERscores(tweets):
    scores_vader = []
    analyzer = SentimentIntensityAnalyzer()
    for tw in tweets:
        scores_vader.append(float(analyzer.polarity_scores(tw['contains'])['compound']))
    return scores_vader

def getResponseVariables(date):
    
    end = stringToDatetime(date) + datetime.timedelta(days=1)
    jsonDataH = getHistoricalPrice(date,end.strftime('%Y-%m-%d'))
    historicalDataset = createHistoricalDataset(jsonDataH)
    historicalDataset_sorted = sorted(historicalDataset, key=lambda k: k['date'])
    for i in range(len(historicalDataset_sorted)-1):
        if historicalDataset_sorted[i]['value'] >= historicalDataset_sorted[i+1]['value']:
            Y = 1
        if historicalDataset_sorted[i]['value'] < historicalDataset_sorted[i+1]['value']:
            Y = 0
    return Y

def getCorpusPerDate(date):
    articles = getGoogleArticle(date)
    corpus = []
    date_publication = ()
    for art in articles['articles']:
        if art['description'] != None:
            description = art['description']
            date_publication = date_publication + (art['publishedAt'],)
            corpus = corpus + cleaningText(description)
    return corpus

def stringToDatetime(date):
    """ Convert a string date to a datetime date
    
    Arguments:
        date {string} -- Date in string format
    
    Returns:
        datetime -- Date in datetime format
    """

    timestemp = int(time.mktime(
        datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))
    return datetime.datetime.fromtimestamp(timestemp)

def getCorpus_between_2_dates(start,end):
    corpus = []
    corpus.append([getCorpusPerDate(start),getResponseVariables(start),start])
    current_date = stringToDatetime(start)
    end_datetime = stringToDatetime(end)
    while current_date < end_datetime:
        current_date += datetime.timedelta(days=1)
        current_date_str = current_date.strftime('%Y-%m-%d')
        corpus.append([getCorpusPerDate(current_date_str),getResponseVariables(current_date_str),current_date_str])
    return corpus

def main():
    sc = SparkContext()
    spark = SparkSession(sc)
    corpus = getCorpus_between_2_dates("2018-03-10","2018-03-22")
    rdd = sc.parallelize(corpus).map(lambda v: Row(text=v[0],label=v[1],date=v[2]))
    df = spark.createDataFrame(rdd)

    df_train, df_test = df.randomSplit([0.7,0.3])


    vectorizer = CountVectorizer(inputCol='text', outputCol="features")
    fit_vect = vectorizer.fit(df_train)

    train_vect = fit_vect.transform(df_train)
    test_vect = fit_vect.transform(df_test)
    
    label_indexer = StringIndexer(inputCol="label", outputCol="label_index")
    fit_ind = label_indexer.fit(train_vect)

    train_indexer = fit_ind.transform(train_vect)
    test_indexer = fit_ind.transform(test_vect)

    classifier = NaiveBayes(labelCol="label_index", featuresCol="features", predictionCol="label_index_predicted")
    fit_class = classifier.fit(train_indexer)

    #train_classifier = fit_class.transform(train_indexer)
    test_classifier = fit_class.transform(test_indexer)

    
    evaluator = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="label_index_predicted", metricName="accuracy")
    accuracy = evaluator.evaluate(test_classifier)
    print("Accuracy: " + str(accuracy))
    #pipeline = Pipeline(stages=[vectorizer, label_indexer, classifier])
    #
    #pipeline_model = pipeline.fit(training)
    #test_predicted = pipeline_model.transform(test)

if __name__ == "__main__":
    main()