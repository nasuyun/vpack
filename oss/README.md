# oss repository plugin

## 创建资源

PUT _snapshot/oss
{
	"type": "oss",
	"settings": {
		"endpoint": "xxx",
		"access_key_id": "xxx",
		"access_key_secret": "xxx",
		"bucket_name": "xxx",
		"base_path": "xxx"
	}
}


## 自动调度

PUT _snapshot/oss/_auto
{
	"interval": "1m",
	"retain": 3
}


## 查看snapshot

GET _snapshot/oss/*


## 恢复

POST _snapshot/oss/{spanshot_id}/_restore

#### copyright by nasuyun.com