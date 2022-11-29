import os
import sys
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()

if os.getenv('ES_PASSWORD') is None or os.getenv('ES_FINGERPRINT') is None:
    print('Env. variables ES_PASSWORD and ES_FINGERPRINT are required')
    sys.exit(1)

client = Elasticsearch(
    "https://localhost:9200",
    ssl_assert_fingerprint=os.getenv('ES_FINGERPRINT'),
    basic_auth=("elastic", os.getenv('ES_PASSWORD'))
)

if client.ping():
    print('Connected to Elasticsearch')
else:
    print('Could not connect to Elasticsearch.')
    sys.exit(1)
