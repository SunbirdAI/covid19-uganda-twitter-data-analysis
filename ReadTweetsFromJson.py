import io
import json


def get_tweets_from_json_file(filename):
    with io.open(filename, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
        f.close()
    return json_data['tweets']


def get_tweet_text(tweet):
    if 'full_text' in tweet:
        return tweet['full_text']
    if 'text' in tweet:
        return tweet['text']
    return ""


def convert_to_annotation_file(tweets):
    with io.open('annotation_tweets.json', 'w', encoding='utf-8') as f:
        for tweet in tweets:
            f.write(json.dumps({"text": get_tweet_text(tweet), "labels": []}, ensure_ascii=False))
            f.write("\n")
        f.close()


if __name__ == '__main__':
    # This is the list of tweets
    _tweets = get_tweets_from_json_file('mask_tweets_v4.json')
    print(len(_tweets))
    convert_to_annotation_file(_tweets)
