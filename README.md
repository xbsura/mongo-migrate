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

## 已知 bug
1. 拷贝索引时,文本索引不能正常建立
