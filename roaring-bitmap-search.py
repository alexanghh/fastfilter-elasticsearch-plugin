from pyroaring import BitMap
import base64
import json
import ast


from elasticsearch import Elasticsearch


if __name__ == "__main__":
  es = Elasticsearch("https://localhost:9200/",
                     api_key="UW8yank1QUJMMzMzYzBUQi0zZkc6REw2UUhCcEFSRENiRkUySzhxRXRfZw==",
                     verify_certs=False)
  # in the real world, the bitmap serialization would be for a big list
  # and you could even compute them before and store them for later user
  bm = BitMap([30])

  # this will match "foo" in important field
  # but only for documents that have a filter_id value of 10, 20 or 35
  terms_bytes = base64.b64encode(BitMap.serialize(bm))
  terms_encoded = terms_bytes.decode()
  print(terms_bytes)

  result = es.search(
    index="test",
    body={
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
