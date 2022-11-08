# Elasticsearch XDCR (Cross DataCenter Replication ) 

提供跨集群复制支持，满足场景：
异地高可用 (HA)：对于许多任务核心链路应用程序，都需要能够承受住数据中心或区域服务中断的影响。通过 XDCR 中的原生功能即可满足跨数据中心的 DR/HA 要求，且无需其他技术。
就近访问：将数据复制到更靠近用户或应用程序服务器的位置，可以减少延迟，降低成本。例如，可以将产品列表或参考数据集复制到全球 20 个或更多数据中心，最大限度地缩短数据与应用程序服务器之间的距离。

## 使用样例

###  主集群建立索引
> 注意：index_settings 必须开启 soft_deletes

```json
PUT index-1
{
	"settings": {
		"index": {
			"soft_deletes.enabled": true,
			"number_of_shards": 1,
			"number_of_replicas": 0
		}
	}
}
```

### 备集群上建立远程仓库(repository)连接到主集群

```json
PUT _cluster/settings  
{
  "persistent": {
    "cluster": {
      "remote": {
        "leader": {  // repository name
          "seeds": [
            "127.0.0.1:9300"  // repository seed
          ]
        }
      }
    }
  }
}
```


### 操作示例 (On 备集群)

- 开始同步索引
```
PUT /_xdcr/{repository}/{index}
```

- 停止索引级同步
```
DELETE /_xdcr/{repository}/{index}
```

- 仅做远程恢复索引，不做同步
```
_xdcr/_recovery/{repository}/{index}
```

- 开启集群级别同步
```
POST /_xdcr/{repository}
```

- 仅关闭集群巡检任务。
```
DELETE /_xdcr/{repository}
```

- 集群级别同步进阶
```
POST /_xdcr/{repository}?excludes=index-1

// 停止所有索引同步
excludes:* 

// 停止index*索引同步
excludes:index* 

// 停止点号开头的索引同步
excludes:.* 

// 匹配到index-1,index-2，kibana的索引停止同步
excludes:index-1,index-2,*kibana*

```

查看状态

```
GET _cat/xdcr

index           repository shard localMaxSeqNo remoteMaxSeqNo
foo.logs-221998 cloud_dev  0     891889        891889
foo.logs-221998 cloud_dev  1     891258        891258
foo.logs-221998 cloud_dev  2     893193        893193
foo.logs-221998 cloud_dev  3     894165        894165
foo.logs-221998 cloud_dev  4     892704        892704
foo.logs-221998 cloud_dev  5     893289        893289
foo.logs-221998 cloud_dev  6     893017        893017
foo.logs-221998 cloud_dev  7     894805        894805

```

```json
_xdcr/stats

{
	"isPeer": true,
	"notPeer": [],
	"stats": [
		{
			"repository": "cloud_dev",
			"index": "foo.logs-221998",
			"shard": 0,
			"local": {
				"seq_no": {
					"max_seq_no": 891889,
					"local_checkpoint": 891889,
					"global_checkpoint": 891889
				}
			},
			"remote": {
				"seq_no": {
					"max_seq_no": 891889,
					"local_checkpoint": 891889,
					"global_checkpoint": 891889
				}
			}
		},
	    ...
	]
}
```

#### copyright by nasuyun.com