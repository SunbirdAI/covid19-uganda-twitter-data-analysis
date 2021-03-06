from data_collection.twitterv2.db_utils import retrieve_tweets, fetch_tweet_by_id, fetch_all_users
from data_collection.targeted_search.search import USERNAMES_DICT
import io
import json
import re
from nltk.tokenize import word_tokenize
from typing import List

MODE = "moh_engagement"
COVID_WORDS = ["covid", "covid19", "corona", "coronavirus", "mask", "masks", "lockdown",
               "staysafe", "virus", "cov", "stayhome", "staysafeug", "socialdistance",
               "washyourhands", "wearamask", "covid-19"]


def get_tweet_words(tweet_text: str) -> List[str]:
    """Clean up tweet"""
    tweet = tweet_text.lower()
    tweet = re.sub(r'((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet)  # remove URLs
    tweet = re.sub(r'@[^\s]+', 'AT_USER', tweet)  # remove usernames
    tweet = re.sub(r'#([\w]+)', '', tweet)  # remove the # in #hashtag
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


def filter_tweets_by_date(tweets, since_date):
    relevant_tweets = []
    for tweet in tweets:
        if tweet['created_at'] > since_date:
            relevant_tweets.append(tweet)
    return relevant_tweets


def filter_relevant_users(tweets):
    relevant_users = set(USERNAMES_DICT[MODE])
    new_tweets = []
    for tweet in tweets:
        author_id = tweet['author_id']
        if author_id in _users_index:
            username = _users_index[author_id]['username']
            if username in relevant_users:
                new_tweets.append(tweet)
    return new_tweets


def create_user_index(users_list):
    user_index = dict()
    for user in users_list:
        user_index[user['id']] = user
    return user_index


def get_users_name(user_id, users_index):
    if user_id in users_index:
        user = users_index[user_id]
        return "{} @{}".format(user['name'], user['username'])
    return user_id


def create_tweet_text_for_annotation(tweet, original_tweet=False):
    # TweetID\nAuthorName @username\n\nTweetText\n\n(If reply)Replying to\n recurse(Original tweet)
    tweet_id_text = "TweetID: {}".format(tweet['id'])
    user = "User: {}".format(get_users_name(tweet['author_id'], _users_index))
    date = "Date: {}".format(tweet['created_at'].replace("T", " at "))
    text = tweet['text']

    parent_tweet = ""
    if not original_tweet and 'referenced_tweets' in tweet:
        ref_tweet = tweet['referenced_tweets'][0]
        if ref_tweet['type'] == 'replied_to':
            tweet_id = ref_tweet['id']
            matching_tweets = fetch_tweet_by_id(tweet_id, "targeted")
            if len(matching_tweets) > 0:
                parent_tweet = create_tweet_text_for_annotation(matching_tweets[0], True)

    tweet_text = "{}\n{}\n{}\n\n{}".format(tweet_id_text, user, date, text)
    if parent_tweet != "":
        tweet_text += "\n\nReplying to:\n{}".format(parent_tweet)
    return tweet_text


def create_annotation_file_from_tweets(tweets_to_write):
    with io.open("{}_tweets.json".format(MODE), 'w', encoding='utf-8') as f:
        for tweet in tweets_to_write:
            non_covid_text = filter_covid_words(tweet['text'])
            if len(non_covid_text) > 0:
                f.write(json.dumps({"id": tweet['id'], "text": non_covid_text, "labels": []}, ensure_ascii=False))
                f.write("\n")
        f.close()


def create_tweet_for_df(tweet):
    user = get_users_name(tweet["author_id"], _users_index)
    df_tweet = {"username": user, "user_id": tweet["author_id"]}
    fields = ["id", "text", "source", "created_at", "public_metrics",
              "in_reply_to_user_id", "referenced_tweets", "labels"]
    for field in fields:
        if field in tweet:
            df_tweet[field] = tweet[field]
    return df_tweet


def create_json_file(tweets_to_write):
    with io.open("{}_analysis_tweets.json".format(MODE), 'w', encoding='utf-8') as f:
        tweets_json = []
        for tweet in tweets_to_write:
            tweets_json.append(create_tweet_for_df(tweet))
        f.write(json.dumps({"tweets": tweets_json}, ensure_ascii=False, indent=4))
        f.close()


if __name__ == '__main__':
    _tweets = sorted(retrieve_tweets(mode=MODE), key=lambda tweet: tweet['created_at'])
    _users_list = fetch_all_users(mode=MODE)
    _users_index = create_user_index(_users_list)
    _tweets = filter_relevant_users(_tweets)
    print(len(_tweets))
    _tweets = filter_tweets_by_date(_tweets, "2020-10-30")
    print("Number of tweets: {}".format(len(_tweets)))
    # print(tweets[100])
    # print(create_tweet_text_for_annotation(tweets[100]))
    print("========================================================")
    # tweet_to_display = len(_tweets) - 1
    # print(create_tweet_text_for_annotation(_tweets[tweet_to_display]))
    # _tweets[tweet_to_display].pop('_id')
    # print(json.dumps(_tweets[tweet_to_display], indent=4))

    # print(tweets[0]['created_at'])
    # print(tweets[-1]['created_at'])
    create_json_file(_tweets)
    # print(fetch_user_by_id("1249028558", mode="targeted"))
    # tweets = filter_tweets_by_date(tweets, "2020-08-17")
    # print("Number of tweets")
    # create_annotation_file_from_tweets(_tweets)
