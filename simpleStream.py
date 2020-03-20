import tweepy
from pymongo import MongoClient
from _datetime import timedelta
from datetime import datetime
import json
import time
import pymongo.errors 
import twitterAuth



# Setup database connection
client = MongoClient()
print(client.list_database_names())

db = client.twitterStream


RUN_TIME = 60 # minutes
COLLECTION_NAME = "small" # Mongo collection name, which holds tweets



auth = tweepy.OAuthHandler(twitterAuth.consumer_key, twitterAuth.consumer_secret)
auth.set_access_token(twitterAuth.access_token, twitterAuth.access_token_secret)

api = tweepy.API(auth)

duplicates = 0

    
# Stream handler
class StreamListener(tweepy.StreamListener):
    
    def on_status(self, status):
        global duplicates
        try:
            db[COLLECTION_NAME].insert_one(convert_date(status))
        except pymongo.errors.DuplicateKeyError:
            duplicates += 1
            
        print(status.text)
        
    def on_error(self, status_code):
        if status_code == 420:
            return False
    
    def on_limit(self, track):
        print("Limit reached")
        return False
    

def convert_date(status):
    tweet = status._json
    tweet["created_at"] = datetime.strptime(tweet["created_at"], '%a %b %d %H:%M:%S +0000 %Y')
    tweet["_id"] = tweet["id"]
    return tweet
    
    
twitterStream = tweepy.Stream(auth=api.auth, listener = StreamListener())
twitterStream.sample(languages=["en"], is_async=True)

        
# Run for specified time
start_time = datetime.now()
end_time =  start_time + timedelta(minutes=RUN_TIME)


while datetime.now() < end_time:
    time.sleep(10)

twitterStream.disconnect()

print("Number of duplicates", duplicates)
