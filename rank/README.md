## RankedQuery

RankedQuery 在原始查询上增加了一层结果集的自定义排位功能，通常用于 BadCase 的快速处理或垂搜场景的结果集处理。

> 利用 RankedQuery 搭配一个上层规则系统可以构建一个简单的排位系统。

### 用法示例

```json
GET {index}/_search
{
    "query": {
        "ranked" : {
            "query" : {
                ... 原始query
            },
            "rank" : {
                "top": [ "_id1", "_id2" ],
                "block" : [ "_id3" ],
                "pos": [ {"_id4" : 3}, {"_id5" : 8} ] 
            }
        }
    }
}
```

#### 使用说明

[_id](https://www.elastic.co/guide/en/elasticsearch/reference/6.6/mapping-id-enabled.html) 为文档主键。
- **block** 在结果集中排除某些文档。
- **top** 尽量将文档置顶。
- **pos** 精确指定文档排位，优先级高于 top。

#### 限制说明

- block 最多处理 1024 个文档。
- top+pos 合计最多处理 1024 个文档。
- 被处理的文档将丧失原始分 _score。


### 多索引示例

指定索引级文档的排序位置

```json
GET index*/_search
{
    "query": {
        "ranked" : {
            "query" : {
                "match_all": {}
            },
            "rank" : {
                "top": [ "index3#1", "index2#1", "index1#1" ],
                "block" : [ "index2#2" ],
                "pos": [ 
                  {"index3#2" : 1}, 
                  {"index3#3" : 5},
                  {"index2#3" : 6}
                ] 
            }
        }
    }
}

```