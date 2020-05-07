import tweepy
import socket
import re
import preprocessor

# Enter your Twitter keys here!!!

ACCESS_TOKEN = "1253174517255606272-7T6XvB4M7NFpwCePdNirpG4fi60FDK"
ACCESS_SECRET = "vL96lgst7KHOKPjtSUWK9Btmyl3hxM4RvcscN5rTxttq4"
CONSUMER_KEY = "PEiQyqJDJugX0bdXVzWNHE0ZF"
CONSUMER_SECRET = "R27hiG8mgy1xlAFzScyklrhgLIhkdKHIVNUf3Ww6Ucf7VJUQBm"


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
hashtag = '#covid19'

TCP_IP = 'localhost'
TCP_PORT = 9001

def preprocessing(tweet):
   # Add here your code to preprocess the tweets and 
   # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc


   EMOJI_PATTERN = re.compile(
    "["
    "\U0001F1E0-\U0001F1FF"  # flags (iOS)
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
    "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
    "\U0001FA00-\U0001FA6F"  # Chess Symbols
    "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
    "\U00002702-\U000027B0"  # Dingbats
    "]+", flags=re,UNICODE)

   tweet = re.sub(r'[^\x00-\x7F]+',' ', tweet)
   tweet = EMOJI_PATTERN.sub(r'', tweet)
   return preprocessor.clean(tweet)

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
           # print(status.text)
           conn.send(tweetLocation.encode('utf-8'))
    return True

  def on_error(self, status_code):
    if status_code == 420:
      return False
    else:
      print(status_code)



myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"])
