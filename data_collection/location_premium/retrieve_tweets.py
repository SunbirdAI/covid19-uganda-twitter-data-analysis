from data_collection.twitterv2.db_utils import retrieve_tweets
import json


if __name__ == '__main__':
    tweets = retrieve_tweets("ugandan")
    print(len(tweets))
    first_tweet = tweets[0]
    first_tweet.pop('_id', first_tweet)
    print(json.dumps(first_tweet, indent=4))
