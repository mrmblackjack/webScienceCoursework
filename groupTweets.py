from pymongo.mongo_client import MongoClient
from pymongo.collection import *
import pandas as pd
from textblob import TextBlob
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer 
from sklearn.cluster import KMeans, MiniBatchKMeans
import math


# Setup database connection
client = MongoClient()

db = client.twitterStream

COLLECTION_NAME = "small"


tweet_collection = db[COLLECTION_NAME]


data = pd.DataFrame(list(tweet_collection.find()))
pd.set_option("display.max_columns", 10)


def textblob_tokenizer(str_input):
    blob = TextBlob(str_input.lower())
    tokens = blob.words
    words = [token.stem() for token in tokens]
    return words



vec = TfidfVectorizer(tokenizer=textblob_tokenizer,
                      stop_words='english',
                      use_idf=True)

x =  data["text"].values

matrix = vec.fit_transform(x)

number_of_clusters=  math.floor(matrix.size * 0.01) 


km = MiniBatchKMeans(n_clusters=number_of_clusters)

km.fit(matrix)

data["cluster"] = km.labels_

print()
print("==================================")
print("Tweet Dataset size: ", matrix.size)
print("Number of clusters (10%K): ", number_of_clusters)


print("==================================")
print()


for i in range(number_of_clusters): # For each cluster
    df = data[data["cluster"] == i]
    
    print ("Cluster #: ", i)
    print ("Size of cluster: ", len(df))
    
    users = df["user"].values
    
    for user in users:
        print("UserID: ", user["id"])
    
    print("-------------------")

 
    





