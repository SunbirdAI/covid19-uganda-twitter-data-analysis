import requests
import json
import logging
from datetime import datetime

from data_collection.twitterv2.db_utils import retrieve_tweets, update_tweet
from data_collection.twitterv2.twitter_api import auth
from ReadTweetsFromJson import grouper
from logging_utils import create_logger

SUNBERT_API_URL = f"{auth('SUNBERT_API')}/predict_batch"
logger = create_logger(__name__,
                       filename='classification.log',
                       logging_format="%(asctime)s: %(name)s: %(levelname)s: %(message)s",
                       logging_level=logging.DEBUG)

start_time = datetime.now().timestamp()


def calculate_time_elapsed():
    curr_time = datetime.now().timestamp()
    seconds = curr_time - start_time
    minutes = seconds/60
    hours = minutes/60
    logger.debug(f"Time elapsed: {seconds} seconds == {minutes} minutes == {hours} hours")


def make_json_data(tweet):
    data = {
        'id': str(tweet['id']),
        'text': tweet['text']
    }

    return data


def make_request(tweets):
    json_tweets = [make_json_data(tweet) for tweet in tweets]
    request_data = {"text_list": json_tweets}
    response = requests.post(SUNBERT_API_URL, json.dumps(request_data))
    return response.json()


def update_db_record(prediction):
    tweet_id = prediction['id']
    class_field = {
        'prediction': prediction['prediction']
    }
    update_tweet(tweet_id, class_field, mode='ugandan')


def classify_tweets(tweets, batch_size):
    # classifies tweets in batches of 100
    total_tweets_done = 0
    for i, group in enumerate(grouper(tweets, batch_size)):
        batch_tweets = [tweet for tweet in group if tweet is not None and 'prediction' not in tweet]
        predictions = make_request(batch_tweets)
        for prediction in predictions:
            update_db_record(prediction)
        total_tweets_done += len(batch_tweets)
        if i % 50 == 0:
            logger.debug(f"Total tweets classified: {total_tweets_done}")
            calculate_time_elapsed()


if __name__ == '__main__':
    tweets_ = retrieve_tweets("ugandan")
    classify_tweets(tweets_, 100)
    # sample_tweets = tweets_[0:10]
    # one_tweet = tweets_[7]
    # tweets_[7].pop('_id')
    # print(f"Before: {json.dumps(one_tweet, indent=4)}")
    # # update_tweet(one_tweet['id'], {'prediction': {'probabilities': {'Non-Covid': 0.9995600581169128,
    # #                                                                 'Covid': 0.0004399318131618202},
    # #                                               'classification': 'Non-Covid', 'confidence': 0.9995600581169128}},
    # #              mode="ugandan")
    # # classify_tweets(sample_tweets)
    # tweets_ = retrieve_tweets("ugandan")
    # # sample_tweets = tweets_[0:200]
    # tweets_[7].pop('_id')
    # print(f"After: {json.dumps(tweets_[7], indent=4)}")
    # classify_tweets(tweets_[0:10])
