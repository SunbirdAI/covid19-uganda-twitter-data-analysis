from nltk.corpus import stopwords
from string import punctuation
from pathlib import Path


STOP_WORDS = set(stopwords.words('english') + list(punctuation) + ['AT_USER', 'URL'])
ANALYSIS_ROOT = Path(__file__).resolve().parent
DATA_FOLDER = ANALYSIS_ROOT.joinpath("data")
