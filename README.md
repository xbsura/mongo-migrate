# mongo-migrate 工具使用介绍

---

## 功能
将一个集群,复制集或者单机 mongoDB 中的一个集合的数据,迁移到另一个集群,复制集或者单机 mongoDB.
如果源集合使用了分片,会在目标自动分片,并按照源集合的 chunk 进行 split 与随机 movechunk.
如果源集合存在索引,会拷贝索引.
如果源使用了复制集,会使用 oplog 做增量迁移.

## 参数
--src "ip:host" 源地址
--srcDB "db name" 要迁移的数据库名称
--srcColl "collection name" 要迁移的集合名称
--srcUserName "user name" 源 auth 用户名
--srcPassWord "password" 源 auth 密码
--dest "ip:host" 目标地址
--destDB "db name" 目标数据库名称
--destColl "collection name" 目标集合名称
--destUserName "user name" 目标 auth 用户名
--destPassWord "password" 目标 auth 密码
--findAndInsertWorkerNum 10 拷贝数据时并发读写goroutine 数目

--writeAck 1 "same with w param in other driver"
--writeMode "Majority" "see mongodb doc if needed"
--journal  true "whether use journal"
--fsync false "whether wait for fsync for each write"

## 已知 bug
1. 拷贝索引时,文本索引不能正常建立

## 范围迁移使用
范围迁移为应对数据量较多时的方法,使用有许多限制,相关参数如下:
--minKey 范围最小值(包含)
--maxKey 范围最大值(不包含)
--keyType "int or string" key 的类型,只支持数字与字符串
这里的范围是进行 shardCollection 时第一个 key 的范围
范围迁移的一些行为:

* 如果目标 ns 已经分片,则不进行分片 **以及** 预分配操作.
* 如果指定了范围为 int 类型的0 - 100 ,则所有非 int 型的数据不能被迁移.
* int 型包含 int64与 float64,对应于在 mongoDB 中直接写入{ key:1 } 与 {key:NumberLong:1}

额外参数,-- withOutKeyType string
一般用来配合范围迁移使用,使用场景:
有集合, shardkey 的第一个 key 99% 都为数字,先通过范围迁移: 0-100,100-200...将99% 数据迁移.
再使用非范围迁移,并指定 --withOutKeyType "int" 将剩余部分数据迁移.
