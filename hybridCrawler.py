import tweepy
from pymongo import MongoClient
from _datetime import timedelta
from datetime import datetime
import json
import time
from collections import Counter
import pymongo.errors 
import twitterAuth


# Setup database connection
client = MongoClient()

db = client.twitterStream


RUN_TIME = 60 # minutes
COLLECTION_NAME = "small" # Mongo collection name, which holds tweets

user_counter = Counter()
duplicates = 0

auth = tweepy.OAuthHandler(twitterAuth.consumer_key, twitterAuth.consumer_secret)
auth.set_access_token(twitterAuth.access_token, twitterAuth.access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

    
# Stream handler
class StreamListener(tweepy.StreamListener):
    
    def on_status(self, status):
        global duplicates
        global user_counter
        user_counter[status.user.id] += 1
        
        try:
            db[COLLECTION_NAME].insert_one(convert_date(status))
        except pymongo.errors.DuplicateKeyError:
            duplicates += 1
        
    def on_error(self, status_code):
        if status_code == 420:
            return False
    
    
def convert_date(status):
    tweet = status._json
    tweet["created_at"] = datetime.strptime(tweet["created_at"], '%a %b %d %H:%M:%S +0000 %Y')
    tweet["_id"] = tweet["id"]
    return tweet
    

def process_user(user_id):
    global duplicates
    global user_counter
    for status in tweepy.Cursor(api.user_timeline, id=user_id).items():
        try:
            db[COLLECTION_NAME].insert_one(convert_date(status))
        except pymongo.errors.DuplicateKeyError:
            duplicates += 1
       
    
twitterStream = tweepy.Stream(auth=api.auth, listener = StreamListener())
twitterStream.sample(languages=["en"], is_async=True) 


   
# Run for specified time
start_time = datetime.now()
end_time = start_time + timedelta(minutes=RUN_TIME)


while datetime.now() < end_time:
    time.sleep(10)

twitterStream.disconnect()

for count in user_counter.most_common(10):
    print("PROCESSING USER + ", count)
    process_user(count[0])


print("Number of duplicates", duplicates)


