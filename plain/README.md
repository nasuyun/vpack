# plain plugin

表格化输出，将_search替换成_query即可。

```
POST /_query

POST /{index}/_query

POST /{index}/{type}/_query
```

```json
GET my-index/_query
{
  "query": {
    "match": {
      "message": "GET /search"
    }
  }
  "sort": [
    {
      "http.response.bytes": { 
        "order": "desc"
      }
    }
  ],
  "from": 0                    
}
```
