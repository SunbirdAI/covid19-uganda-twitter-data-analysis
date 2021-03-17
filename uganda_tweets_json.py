import io
import json
import os
import shutil
import time
from definitions import DATA_DIR
from constants import DATA_FOLDER

from ReadTweetsFromJson import grouper
from data_collection.twitterv2.db_utils import retrieve_tweets
from save_ug_tweets_to_db import divide_tweets_into_months


def delete_files():
    print("Waiting for 2 minutes, transfer files quickly before they are deleted")
    time.sleep(120)
    folder = 'data'
    for file_name in os.listdir(folder):
        file_path = os.path.join(folder, file_name)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def write_covid_tweets_to_json(file, covid_tweets, total_tweets, total_covid_tweets):
    with io.open(file, 'w', encoding='utf-8') as f:
        serializable_tweets = []
        for tweet in covid_tweets:
            tweet.pop('_id')
            serializable_tweets.append(tweet)
        data = {
            'total_tweets': total_tweets,
            'covid_tweets': total_covid_tweets,
            'tweets': serializable_tweets
        }
        f.write(json.dumps(data, ensure_ascii=False, indent=5))


def get_classified_covid_tweets(tweets, chunk_size=300000):
    covid_tweets = [tweet for tweet in tweets if tweet['prediction']['classification'] == 'Covid']
    print(f'Total classified tweets: {len(tweets)}')
    print(f'Total covid tweets: {len(covid_tweets)}')
    monthly_tweets = divide_tweets_into_months(tweets)
    for month in monthly_tweets:
        if month in months_done:
            continue
        month_tweets = monthly_tweets[month]
        month_cov_tweets = [tweet for tweet in month_tweets if tweet['prediction']['classification'] == 'Covid']
        json_directory = f"{DATA_DIR}/{month}"
        if not os.path.exists(json_directory):
            os.makedirs(json_directory)
        if len(month_cov_tweets) < chunk_size:
            write_covid_tweets_to_json(f"{json_directory}/covid_tweets.json", month_cov_tweets,
                                       len(month_tweets), len(month_cov_tweets))
        else:
            for i, group in enumerate(grouper(month_cov_tweets, chunk_size)):
                cov_tweets = [tweet for tweet in group if tweet is not None]
                write_covid_tweets_to_json(f"{json_directory}/covid_tweets_{i + 1}.json", cov_tweets,
                                           len(month_tweets), len(month_cov_tweets))
                if (i + 1) % 10 == 0:
                    delete_files()
                    os.makedirs(json_directory)
        delete_files()


if __name__ == '__main__':
    tweets_ = retrieve_tweets("ugandan")
    # creating the json file
    # tweets = []
    # for tweet in tweets_id:
    #     tweet.pop('_id')
    #     tweets.append(tweet)
    # for i, group in enumerate(grouper(tweets, 20000)):
    #     filename = f'data/ug_tweets_{i + 1}.json'
    #     with io.open(filename, 'w', encoding='utf-8') as f:
    #         f.write(json.dumps({"tweets": list(group)}, ensure_ascii=False, indent=5))
    #     if (i + 1) % 10 == 0:
    #         print("Waiting for 3 minutes, transfer files quickly before they are deleted")
    #         time.sleep(120)
    #         delete_files()

    tweets_ = [tweet_ for tweet_ in tweets_ if 'prediction' in tweet_]
    months_done = {'2021-2', '2021-1', '2020-12', '2020-11', '2020-10'}
    get_classified_covid_tweets(tweets_)
