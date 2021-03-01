import io
import json

from itertools import zip_longest


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def create_annotation_file_from_tweets(tweets, filename):
    with io.open(filename, 'w', encoding='utf-8') as f:
        for tweet in tweets:
            f.write(json.dumps(tweet, ensure_ascii=False))
            f.write("\n")
        f.close()


def split_json(input_tweets):
    for i, group in enumerate(grouper(input_tweets, 3000)):
        filename = 'influencers_{}.json'.format(i)
        create_annotation_file_from_tweets(list(group), filename)


def get_tweets_from_json_file(filename):
    tweets = []
    with io.open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            tweets.append(json.loads(line))
        f.close()
    return tweets


def get_annotation_tweets(filename):
    with io.open(filename, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
        f.close()
    return json_data


if __name__ == '__main__':
    # This is the list of tweets
    # _tweets = get_tweets_from_json_file('mask_tweets.json'
    _tweets = get_tweets_from_json_file('influencers_tweets.json')
    print(len(_tweets))
    split_json(_tweets)
    # print(len(_tweets))
