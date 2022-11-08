JoinedQuery 从引擎层面实现了跨索引过滤功能，用以简化 ES 在多个索引间的关系处理。 Join基本上是基于共同属性的两组文档之间的semi-join，其中结果仅包含文档集合的一个属性。该连接用于过滤基于第二文档集的一个文档集，相当于SQL中的EXISTS算子。

## 示例

* 主表
  * score : 成绩表
* 子表
  * user : 学生表
  * subject : 科目表

关联关系

```
score.user_id = user.id
score.subject_id = subject.id
```

**注意：** 关联外键类型需使用数值类型（integer或long)

-----

## 建表
```json
PUT score
{
  "mappings": {
    "_doc": {
      "properties": {
        "score": {"type":"integer"},
		"user_id": {"type":"integer"},
		"subject_id": {"type":"integer"}
      }
    }
  }
}

PUT user
{
  "mappings": {
    "_doc": {
      "properties": {
        "id": {"type":"integer"},
		"name": {"type":"keyword"}
      }
    }
  }
}

PUT subject
{
  "mappings": {
    "_doc": {
      "properties": {
        "id": {"type":"integer"},
		"name": {"type":"keyword"}
      }
    }
  }
}
```

## 插入数据

```json
POST user/_doc
{
  "id": 1,
  "name": "jack"
}

POST user/_doc
{
  "id": 2,
  "name": "rose"
}

POST subject/_doc
{
  "id": 1,
  "name": "物理"
}

POST subject/_doc
{
  "id": 2,
  "name": "化学"
}

POST score/_bulk
{ "index" : { "_type" : "_doc" } }
{ "score" : 95, "user_id": 1, "subject_id": 1  }
{ "index" : { "_type" : "_doc" } }
{ "score" : 66, "user_id": 1, "subject_id": 2  }
{ "index" : { "_type" : "_doc" } }
{ "score" : 88, "user_id": 2, "subject_id": 1  }
{ "index" : { "_type" : "_doc" } }
{ "score" : 100, "user_id": 2, "subject_id": 2  }
```

## join检索

### 案例1 - 查看 jack 的成绩表 (单表连接)

```sql
SELECT score.* FROM score
LEFT JOIN user
ON user.id=score.user_id
WHERE user.name='jack'
```

#### By JoinedQuery
```json
GET score/_search
{
	"query": {
		"joined": {
			"query": {
				"match_all": {}
			},
			"join": [{
				"user_id": {
					"indices": "user",
					"enabled": "id",
					"query": {
						"term" : { "name" : "jack" }
					}
				}
			}]
		}
	}
}
```

### 案例2- 查看 jack 同学的物理成绩表 (多子表连接)

```sql
SELECT score.* FROM score
LEFT JOIN user
ON user.id=score.user_id
LEFT JOIN subject
ON subject.id=score.subject_id
WHERE user.name='?' AND subject.name='?'
```

#### By JoinedQuery

```json
GET score/_search
{
	"query": {
		"joined": {
			"query": {
				"match_all": {}
			},
			"join": [
			  {
  				"user_id": {
  					"indices": "user",
  					"enabled": "id",
  					"query": {
  						"term" : { "name" : "jack" }
  					}
  				}
			  },
			  {
  				"subject_id": {
  					"indices": "subject",
  					"enabled": "id",
  					"query": {
  						"term" : { "name" : "物理" }
  					}
  				}
			  }
			]
		}
	}
}
```

## 重要说明

1. **关联字段类型** 关联外键请使用数值类型（integer或long)。
2. **数据裁剪** 出于对服务端的保护，join操作在取中间结果集时有个阈值保护，默认阈值为100万，超出阈值则中断查询，若发生截断SearchResponse会增加 ealy_terminated:true 提示检索数据被裁剪。
