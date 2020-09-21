from data_collection.twitterv2.db_utils import retrieve_tweets, fetch_user_by_id, fetch_tweet_by_id, fetch_all_users
import io
import json


def filter_tweets_by_date(tweets, since_date):
    pass


def get_users_name(user_id):
    matching_users = fetch_user_by_id(user_id)
    if len(matching_users) > 0:
        user = matching_users[0]
        return "{} @{}".format(user['name'], user['username'])
    return user_id


def create_tweet_text_for_annotation(tweet, original_tweet=False):
    # TweetID\nAuthorName @username\n\nTweetText\n\n(If reply)Replying to\n recurse(Original tweet)
    tweet_id_text = "TweetID: {}".format(tweet['id'])
    user = "User: {}".format(get_users_name(tweet['author_id']))
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
    with io.open("kcca_tweets.json", 'w', encoding='utf-8') as f:
        for tweet in tweets_to_write:
            text = create_tweet_text_for_annotation(tweet)
            labels = []
            f.write(json.dumps({"text": text, "labels": labels}, ensure_ascii=False))
            f.write("\n")
        f.close()


if __name__ == '__main__':
    tweets = sorted(retrieve_tweets(mode="targeted"), key=lambda tweet: tweet['created_at'])
    # print("Number of tweets: {}".format(len(tweets)))
    # print(tweets[100])
    # print(create_tweet_text_for_annotation(tweets[100]))
    print("========================================================")
    print(create_tweet_text_for_annotation(tweets[0]))
    # print(tweets[0]['created_at'])
    # print(tweets[-1]['created_at'])

    # print(fetch_user_by_id("1249028558", mode="targeted"))
    # tweets = filter_tweets_by_date(tweets, "2020-08-17")
    # print("Number of tweets")
    create_annotation_file_from_tweets(tweets)
