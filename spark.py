from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from bs4 import BeautifulSoup
from nltk.tokenize import WordPunctTokenizer
import json
import pickle
from geopy.geocoders import Nominatim
from textblob import TextBlob
from elasticsearch import Elasticsearch
import re

geolocator = Nominatim(user_agent='tweet')

TCP_IP = 'localhost'
TCP_PORT = 9001

def testDisplay(tweet):
    es = ElasticSearch([{ 'host': 'localhost','port': 9200 }])
    tweetData = tweet.split("::")

    if len(tweetData) > 1:
        text = tweetData[1]

        if float(TextBlob(text).sentiment.polarity) > 0.3:
            sentiment = "postive"
        elif float(TextBlob(text).sentiment.polarity) < -0.3:
            sentiment = "negative"
        else:
            sentiment = "neutral"

        try:
            location = geolocator.geocode(tweetData[0], addressdetails=True)
            lat = location.raw['lat']
            lon = location.raw['lon']
            state = location.raw['address']['state']
            country = location.raw['address']['country']
        except:
            lat=lon=state=country=None


        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!) 
        if lat != None and lon != None and sentiment != None:
            esDoc = {"lat":lat, "lon":lon, "state": state, "country":country, "sentiment":sentiment}
            es.index(index='tweet-sentiment', doc_type='default', body=esDoc)

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext.getOrCreate(conf=conf)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


dataStream.foreachRDD(lambda rdd: rdd.foreach(testDisplay))

ssc.start()
ssc.awaitTermination()
