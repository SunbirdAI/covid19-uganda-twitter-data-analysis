from data_collection.twitterv2.twitter_api import create_users_url, extract_conversation_ids
from data_collection.twitterv2.db_utils import retrieve_tweets

sample_tweets = [
    {
        "author_id": "344718893",
        "conversation_id": "1300379671801065474",
        "id": "1300379671801065474",
        "public_metrics": {
            "like_count": 30,
            "quote_count": 0,
            "reply_count": 4,
            "retweet_count": 4
        },
        "text": "Bobi Wine says his age was altered from 1982 to 1980 to match other candidates he joined to sit PLE after he was made to skip Primary Six.\n\n#NBSUpdates #UgVotes2021 #NBSPoliticom #NBSLiveAt1 https://t.co/kgvWX8bXc8"
    },
    {
        "author_id": "344718893",
        "conversation_id": "1300379387838312449",
        "id": "1300379387838312449",
        "public_metrics": {
            "like_count": 15,
            "quote_count": 0,
            "reply_count": 3,
            "retweet_count": 4
        },
        "text": "FDC's Patrick Oboi Amuriat has faulted the Ministry of Health for introducing a fee for the #COVID19 testing saying Ugandans cannot afford it. According to Amuriat, commercialization of #COVID19 testing will make people poorer.\n\n#NBSUpdates #UgVotes2021 #NBSPoliticom #NBSLiveAt1 https://t.co/0aqyaWtx5V"
    },
    {
        "author_id": "344718893",
        "conversation_id": "1300378839714091011",
        "id": "1300379001647751168",
        "public_metrics": {
            "like_count": 5,
            "quote_count": 0,
            "reply_count": 0,
            "retweet_count": 0
        },
        "text": "These include; the handling of the yellow book, violence, and voter bribery.\n\n#NBSUpdates #UgVotes2021 #NBSPoliticom #NBSLiveAt1"
    },
    {
        "author_id": "344718893",
        "conversation_id": "1300378839714091011",
        "id": "1300378839714091011",
        "public_metrics": {
            "like_count": 9,
            "quote_count": 1,
            "reply_count": 1,
            "retweet_count": 0
        },
        "text": "Members of the NRM are calling out to the party's Electoral Commission to iron out patent issues that are already portraying the electoral process as unfair ahead of the Primaries this Friday. \n\n#NBSUpdates #UgVotes2021 #NBSPoliticom #NBSLiveAt1 https://t.co/nbxmhsPSj4"
    },
    {
        "author_id": "344718893",
        "conversation_id": "1300378359025856513",
        "id": "1300378359025856513",
        "public_metrics": {
            "like_count": 15,
            "quote_count": 1,
            "reply_count": 2,
            "retweet_count": 2
        },
        "text": "After weeks of hurling insults at leaders and supporters of the National Unity Platform, political commentator, Basajja Mivule has done what many had suspected he would: do, join the ruling National Resistance Movement.  #NBSUpdates\n\nRead the story: https://t.co/SWlsenQtP0"
    },
    {
        "author_id": "344718893",
        "conversation_id": "1300377998986817536",
        "id": "1300378120759975937",
        "public_metrics": {
            "like_count": 19,
            "quote_count": 0,
            "reply_count": 3,
            "retweet_count": 2
        },
        "text": "Bobi Wine: There is no way I would be born on February 12th, 1980, only four months after my elder brother's birth.\n\n#NBSUpdates #UgVotes2021 #NBSPoliticom"
    },
    {
        "author_id": "344718893",
        "conversation_id": "1300377998986817536",
        "id": "1300377998986817536",
        "public_metrics": {
            "like_count": 39,
            "quote_count": 1,
            "reply_count": 2,
            "retweet_count": 6
        },
        "text": "Bobi Wine: I was born on February 12th, 1982, in Nkozi. According to all his official records, my brother Julius Walakila, who I follow- same father, same mother, was born on October 23rd, 1979. \n\n#NBSUpdates #UgVotes2021 #NBSPoliticom https://t.co/DtSbGQh8xP"
    },
    {
        "author_id": "344718893",
        "conversation_id": "1300377695122006016",
        "id": "1300377695122006016",
        "public_metrics": {
            "like_count": 5,
            "quote_count": 0,
            "reply_count": 0,
            "retweet_count": 0
        },
        "text": "Heavy rains and strong winds have destroyed over six villages in Kagadi district. This has left locals counting losses, with most crops swept down and houses ruined - Cuthbert kigozi.\n\n#NBSLiveAt1 #NBSUpdates https://t.co/xUsKSJjQJs"
    },
    {
        "author_id": "344718893",
        "conversation_id": "1300373827168460800",
        "id": "1300374062494101507",
        "public_metrics": {
            "like_count": 9,
            "quote_count": 0,
            "reply_count": 0,
            "retweet_count": 0
        },
        "text": "@BenOngomTweets On Besigye\u2019s plan B, Aol says Besigye will involve all parties hoping for change, and a master plan to change gov\u2019t is being assessed for implementation.\n\n@BenOngomTweets \n\n#NBSLiveAt1 #NBSUpdates #UgVotes2021 #NBSPoliticom"
    },
    {
        "author_id": "344718893",
        "conversation_id": "1300373827168460800",
        "id": "1300373964510900230",
        "public_metrics": {
            "like_count": 5,
            "quote_count": 0,
            "reply_count": 1,
            "retweet_count": 0
        },
        "text": "@BenOngomTweets She says the call for a coalition is impossible, and the FDC is not ready to enter any opposition unity coalition. \n\n@BenOngomTweets \n\n#NBSLiveAt1 #NBSUpdates #UgVotes2021 #NBSPoliticom"
    }
]


def test_create_users_url():
    usernames = ["user1", "user2"]
    url = create_users_url(usernames=usernames)
    print("Url for users: {}".format(url))

    ids = ["92182", "471381"]
    url = create_users_url(ids=ids)
    print("Url for ids: {}".format(url))


def test_extract_conversation_ids():
    conv_ids = set()
    conv_ids.update(extract_conversation_ids(sample_tweets))
    print("Conversation ids: {}".format(list(conv_ids)))
    # tweets = retrieve_tweets()
    # print("Conversation ids: {}".format(len(list(extract_conversation_ids(tweets)))))


if __name__ == '__main__':
    test_create_users_url()
    test_extract_conversation_ids()
