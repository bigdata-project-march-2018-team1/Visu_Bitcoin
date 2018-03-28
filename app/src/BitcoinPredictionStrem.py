# -*- coding: utf-8 -*-
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import pprint
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

access_token = '963341451273887744-3GwMyUiWOpB9VKZVE7h9sFmdzERSh3t'
access_token_secret = 'gdkpfHZfFScVgXuejkr7Zd5huh3GSQ0EkHVNvJOwm45CF'
consumer_key = 'xKoEBvY5F3FZxhS4rWwehNY1J'
consumer_secret = 'mZhuIsz2S2FBndxwY9Vpt5g8GozF00q7R7V7yhXKGaJ9xxhoAW'

class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream. 
    This is a basic listener that just prints received tweets to stdout.

    """
    def on_data(self, data):
        #pp = pprint.PrettyPrinter()
        #pp.pprint(json.loads(data))
        #print(json.loads(data)['text'].encode('utf-8'))
        tweet = json.loads(data)['text']
        analyzer = SentimentIntensityAnalyzer()
        vs = analyzer.polarity_scores(tweet)
        print(tweet.encode('utf-8'))
        print(str(vs))
        return True

    def on_error(self, status):
        print(status)

listener = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = tweepy.Stream(auth, listener)
stream.filter(track=['btc','bitcoin','xbt','satochi'],languages=['en'])
