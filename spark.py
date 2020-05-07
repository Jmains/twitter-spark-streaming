from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
from geopy.geocoders import Nominatim
from textblob import TextBlob
import re

geolocator = Nominatim(user_agent='tweet')

TCP_IP = 'localhost'
TCP_PORT = 9001

def testDisplay(tweet):
    
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
