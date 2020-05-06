import tweepy
import socket
import re
# import emoji
import preprocessor as p # pip install tweet-preprocessor

# Enter your Twitter keys here!!!
ACCESS_TOKEN = "1253174517255606272-7T6XvB4M7NFpwCePdNirpG4fi60FDK"
ACCESS_SECRET = "vL96lgst7KHOKPjtSUWK9Btmyl3hxM4RvcscN5rTxttq4"
CONSUMER_KEY = "PEiQyqJDJugX0bdXVzWNHE0ZF"
CONSUMER_SECRET = "R27hiG8mgy1xlAFzScyklrhgLIhkdKHIVNUf3Ww6Ucf7VJUQBm"

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

hashtag = '#covid19'

# set preprocessor options to remove URLs, Emoji's, @'s, Smileys, and Reserved words(RT, FAV) in Tweets
p.set_options(p.OPT.EMOJI, p.OPT.URL, p.OPT.MENTION, p.OPT.SMILEY, p.OPT.RESERVED)

TCP_IP = 'localhost'
TCP_PORT = 9001

def preprocessing(tweet):
    # Add here your code to preprocess the tweets and  
    # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc 
    tweet = p.clean(tweet)
    
    return tweet

def getTweet(status):
    
    # You can explore fields/data other than location and the tweet itself. 
    # Check what else you could explore in terms of data inside Status object

    tweet = ""
    location = ""

    location = status.user.location
    
    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text

    return location, preprocessing(tweet)


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, tweet = getTweet(status)

        if (location != None and tweet != None):
            tweetLocation = location + "::" + tweet+"\n"
            print(status.text)
            conn.send(tweetLocation.encode('utf-8'))

        return True


    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"])


