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

access_token = '963341451273887744-3GwMyUiWOpB9VKZVE7h9sFmdzERSh3t'
access_token_secret = 'gdkpfHZfFScVgXuejkr7Zd5huh3GSQ0EkHVNvJOwm45CF'
consumer_key = 'xKoEBvY5F3FZxhS4rWwehNY1J'
consumer_secret = 'mZhuIsz2S2FBndxwY9Vpt5g8GozF00q7R7V7yhXKGaJ9xxhoAW'

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

def getTweets(auth, start,end):
    api = tweepy.API(auth)
    tweets = []
    search_terms = "#btc", "#Bitcoin"
    for tweet in tweepy.Cursor(api.search,q=search_terms,count=2,\
                           lang="en",\
                           since=start, until=end)\
                           .items():
        current_tweet = {}
        current_tweet['date'] = tweet.created_at
        current_tweet['contains'] = tweet.text
        tweets.append(current_tweet)
    return tweets

def cleaningTweets(tweet):
    tweet = re.sub('[^a-zA-Z@# ]','',tweet)
    current_tweet = set(tweet.split(' '))
    tweet_clean = ' '.join(current_tweet)
    return tweet_clean

def getVADERscores(tweets):
    scores_vader = []
    analyzer = SentimentIntensityAnalyzer()
    for tw in tweets:
        scores_vader.append(float(analyzer.polarity_scores(tw['contains'])['compound']))
    return scores_vader

def main():
    listener, auth = connectionToTwitterAPI()
    #stream = tweepy.Stream(auth, listener)
    #stream.filter(track=['btc','bitcoin','xbt','satochi'],languages=['en'])
    tweets = getTweets(auth, "2014-06-12", "2014-06-15")
    scores_vader = getVADERscores(tweets)
    plt.plot(np.arange(len(scores_vader)),scores_vader)
    plt.show()

if __name__ == "__main__":
    main()