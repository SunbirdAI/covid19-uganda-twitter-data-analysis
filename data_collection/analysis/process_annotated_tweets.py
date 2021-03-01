import re
import json
import io
import csv

from collections import defaultdict
from typing import List
from nltk.tokenize import word_tokenize
from data_collection.analysis.constants import COVID_WORDS


def get_tweet_words(tweet_text: str) -> List[str]:
    """Clean up tweet"""
    tweet = tweet_text.lower()
    tweet = re.sub(r'((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet)  # remove URLs
    tweet = re.sub(r'@[^\s]+', 'AT_USER', tweet)  # remove usernames
    tweet = re.sub(r'#', '', tweet)  # remove the # in #hashtag
    tweet = word_tokenize(tweet)  # remove repeated characters (helloooooooo into hello)

    return [word for word in tweet]


def filter_covid_words(tweet_text: str):
    words = get_tweet_words(tweet_text)
    final_words = []
    for word in words:
        is_covid_word = False
        for covid_word in COVID_WORDS:
            is_covid_word = covid_word in word
            if is_covid_word:
                break
        if not is_covid_word:
            final_words.append(word)
    return " ".join(final_words)


def get_tweets_from_json_file(filename):
    tweets = []
    with io.open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            tweets.append(json.loads(line))
        f.close()
    return tweets


def label_to_category(tweets):
    labels = defaultdict(lambda: 0)
    for tweet in tweets:
        labels[tweet["annotations"][0]["label"]] += 1

    label_list = []
    for key in labels.keys():
        label_list.append((key, labels[key]))
    print(label_list)
    label_list.sort(key=lambda k: k[1])
    return {
        label_list[0][0]: "COVID",
        label_list[1][0]: "Non-COVID"
    }


def process_annotated_tweets(tweets):
    final_tweets = []
    category_map = label_to_category(tweets)
    for tweet in tweets:
        _id = tweet['id']
        category = category_map[tweet["annotations"][0]["label"]]
        text = filter_covid_words(tweet["text"])
        final_tweets.append({
            "id": _id,
            "text": text,
            "category": category
        })
    return final_tweets


def make_csv(tweets: List[dict], filename: str):
    keys = tweets[0].keys()
    with open(filename, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(tweets)
        output_file.close()


if __name__ == '__main__':
    curr_file = "influencers5"
    initial_tweets = get_tweets_from_json_file(f"data/annotated/{curr_file}-annotated.json")
    _final_tweets = process_annotated_tweets(initial_tweets)
    make_csv(_final_tweets, f"data/annotated/{curr_file}-annotated.csv")
