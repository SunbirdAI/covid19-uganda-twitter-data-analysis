import requests
import json
import time

from data_collection.twitterv2.twitter_api import auth, create_headers
from save_ug_tweets_to_db import all_geo_ids
from data_collection.twitterv2.db_utils import save_records, fetch_all_places

GEO_URL = "https://api.twitter.com/1.1/geo/id"


def create_url(place_id):
    return f"{GEO_URL}/{place_id}.json"


def save_places(places):
    save_records(places, 'places', 'ugtweets')


def make_request(url):
    bearer_token = auth("TWITTER_BEARER_TOKEN")
    response = requests.get(url, headers=create_headers(bearer_token))
    # print(json.dumps(response, indent=2))
    return response.json(), response.status_code


def get_places(place_ids):
    places = []
    places_collected = 0
    for place_id in place_ids:
        url = create_url(place_id)
        place_json, status_code = make_request(url)
        while status_code == 429:
            print("Rate limit exceeded, waiting for 10 minutes")
            time.sleep(600)
            place_json, status_code = make_request(url)
        if status_code == 200:
            places.append(place_json)
            places_collected += 1
        else:
            print("Error: ", place_json, status_code)
            break
        if len(places) >= 100:
            print(f"Saving {len(places)} places")
            save_places(places)
            places = []
        if len(places) % 20 == 0:
            print(f"Collected {places_collected} places")
    if len(places) > 0:
        print(f"Saving {len(places)} places")
        save_places(places)
    else:
        print("No places to save")


if __name__ == '__main__':
    # ids = ["01c1626a6a25156a", "000c69ad123213a8"]
    # for id_ in ids:
    #     url_ = create_url(id_)
    #     resp = make_request(url_)[0]
    #     # place type can be neighborhood , city , admin or country
    #     if 'name' in resp:
    #         print(f"Name: {resp['name']} Full name: {resp['full_name']} "
    #               f"Place type: {resp['place_type']} Country: {resp['country']}")
    #         print(json.dumps(resp, indent=4))
    #     else:
    #         print(json.dumps(resp, indent=4))

    # # Fetching the places from the twitter api
    # all_ids = all_geo_ids()
    # get_places(all_ids)

    # Viewing the places
    places_ = fetch_all_places()
    rand_place = places_[0]
    rand_place.pop('_id')
    print(json.dumps(rand_place, indent=4))
    # for place in places_:
    #     print(f"Name: {place['name']} Full name: {place['full_name']} Place type: {place['place_type']}")
