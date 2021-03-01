from nltk.corpus import stopwords
from string import punctuation
from pathlib import Path


STOP_WORDS = set(stopwords.words('english') + list(punctuation) + ['AT_USER', 'URL'])
COVID_WORDS = ["covid", "covid19", "corona", "coronavirus", "mask", "masks", "lockdown",
               "staysafe", "virus", "cov", "stayhome", "staysafeug", "socialdistance",
               "washyourhands", "wearamask", "cases", "covid-19"]

ANALYSIS_ROOT = Path(__file__).resolve().parent
DATA_FOLDER = ANALYSIS_ROOT.joinpath("data")
