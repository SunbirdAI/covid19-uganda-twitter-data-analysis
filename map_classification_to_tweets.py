import csv
import json
from save_ug_tweets_to_db import get_all_tweets, divide_tweets_into_months
from constants import DATA_FOLDER


def make_tweets_dict():
    # Keys are the tweet_id and values are the tweet
    tweets = get_all_tweets()
    tweets_dict = dict()
    for tweet in tweets:
        if tweet is None:
            break
        tweets_dict[tweet['id']] = tweet
    return tweets_dict


def get_classified_tweets():
    csv_file_name = DATA_FOLDER.joinpath('classified_tweets.csv')
    classifications = dict()
    with open(csv_file_name, newline='', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        print(reader.fieldnames)
        rows = list(reader)
        # print(len(list(reader)))
        how_many_unclassified = 0
        for row in rows:
            tweet_id = row['tweet_id']
            if row['confidence'] is None:
                how_many_unclassified += 1
                continue
            confidence = float(row['confidence'])
            classifications[tweet_id] = {
                'classification': 'Covid' if confidence >= 0.9 else 'Non-Covid',
                'confidence': confidence
            }
        print(f"Number of unclassified tweets: {how_many_unclassified}")
    return classifications


def map_tweets_to_classification(tweets_dict: dict, classifications: dict):
    tweets = []

    for tweet_id in tweets_dict.keys():
        if tweet_id in classifications:
            tweet = tweets_dict[tweet_id]
            tweet['prediction'] = classifications[tweet_id]
            tweets.append(tweet)
    return tweets


def write_covid_tweets_to_json(file, covid_tweets, total_tweets, total_covid_tweets):
    with open(file, 'w', encoding='utf-8') as f:
        data = {
            'total_tweets': total_tweets,
            'covid_tweets': total_covid_tweets,
            'tweets': covid_tweets
        }
        f.write(json.dumps(data, ensure_ascii=False, indent=5))


def write_monthly_tweets_to_json(tweets, chunk_size=300000):
    covid_tweets = [tweet for tweet in tweets if tweet['prediction']['classification'] == 'Covid']
    print(f'Total classified tweets: {len(tweets)}')
    print(f'Total covid tweets: {len(covid_tweets)}')
    monthly_tweets = divide_tweets_into_months(tweets)
    for month in monthly_tweets:
        month_tweets = monthly_tweets[month]
        month_cov_tweets = [tweet for tweet in month_tweets if tweet['prediction']['classification'] == 'Covid']
        json_file = DATA_FOLDER.joinpath(f"{month}_covid.json")
        if len(month_cov_tweets) < chunk_size:
            write_covid_tweets_to_json(json_file, month_cov_tweets,
                                       len(month_tweets), len(month_cov_tweets))
        else:
            print(f"{month} - {len(month_cov_tweets)}")


if __name__ == '__main__':
    tweets_dict_ = make_tweets_dict()
    print(f"Original tweets: {len(tweets_dict_)}")
    classifications_ = get_classified_tweets()
    print(f"Classified tweets: {len(classifications_)}")
    print(f"Unclassified tweets: {len(tweets_dict_) - len(classifications_)}")

    classified_tweets = map_tweets_to_classification(tweets_dict_, classifications_)
    write_monthly_tweets_to_json(classified_tweets)
    # covid_tweets_ = [tweet for tweet in classified_tweets if tweet['prediction']['classification'] == 'Covid']
    # print(f"Number of covid tweets: {len(covid_tweets_)}")
