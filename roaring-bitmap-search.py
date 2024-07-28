from pyroaring import BitMap
import base64
import json
import ast
import uuid
import argparse

from elasticsearch import Elasticsearch, helpers

def doc_stream(load_size):
  for i in range(load_size):
    yield {
         '_id': i,
         'body': {
           'important_field': str(uuid.uuid4()),
            'filter_id': [str(i)]
         }
    }


def load_data(load_size=100000000):
  for status_ok, response in helpers.streaming_bulk(es,
                                          index='test',
                                          actions=doc_stream(load_size), 
                                          chunk_size=1000):
    if not status_ok:                                                           
        # if failure inserting, log response                                                  
        print(response)    
  # doc = {
  #        'body': {
  #          'important_field': str(uuid.uuid4()),
  #           'filter_id': [str(0)]
  #        }
  #     }
  # es.index(index='test', document=doc, id=0)


def query_test(bm = BitMap([1])):
  # in the real world, the bitmap serialization would be for a big list
  # and you could even compute them before and store them for later user

  # this will match "foo" in important field
  # but only for documents that have a filter_id value in bm
  terms_bytes = base64.b64encode(BitMap.serialize(bm))
  terms_encoded = terms_bytes.decode()
  print(terms_bytes)

  result = es.search(
    index="test",
    body={
      "size": 10, 
      "query": {
        "bool": {
          "filter": {
            "script": {
              "script": {
                "source": "fast_filter",
                "lang": "fast_filter",
                "params": {
                  "field": "body.filter_id",
                  "operation": "include",
                  "type": "string",
                  "terms": terms_encoded
                }
              }
            }
          }
        }
      }
    }
  )
  resp2 = ast.literal_eval(str(result))
  print(json.dumps(resp2, indent=2, sort_keys=True))


def query_1k():
  bm = BitMap()
  for i in range(1000):
    bm.add(i)
  query_test(bm)

def query_1m():
  bm = BitMap()
  for i in range(1000000):
    bm.add(i)
  query_test(bm)

def query(query_size=1000000):
  bm = BitMap()
  for i in range(query_size):
    bm.add(i)
  query_test(bm)



parser = argparse.ArgumentParser(description='ES roaring bitmap test.')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-l', '--load', action='store_true',
                    help='load data to ES index')
group.add_argument('-q' , '--query', action='store_true',
                    help='query ES using roaring bitmap')
parser.add_argument('-s' , '--size', dest='size', required=False, type=int,
                    help='size to load/query')

if __name__ == "__main__":
  args = parser.parse_args()
  print(args)

  if not args.load and not args.query:
    parser.print_help()
    quit()
  
  es = Elasticsearch("https://localhost:9200/",
                     api_key="a0RrOC1wQUJHUHdSQXZUd09XVkg6MzBtZ3FJSnRSRS1Ma0JFZGtGZlJkZw==",
                     verify_certs=False,
                     request_timeout=600)

  if args.load:
    if args.size:
      load_data(args.size)
    else:
      load_data()
  
  if args.query:
    if args.size:
      query(args.size)
    else:
      query()
    
  # load_data()
  # test_query_1m()
  # test_query_1k()

