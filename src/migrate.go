package main

import (
	"flag"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

func GenMongoDBUri(addr, userName, passWord string) string {
	var mongoDBUri string
	if userName == "" || passWord == "" {
		mongoDBUri = "mongodb://" + addr
	} else {
		mongoDBUri = "mongodb://" + userName + ":" + passWord + "@" + addr
	}
	return mongoDBUri
}

var DoShardCollection bool
var DoOplogSync bool
var LastOpTs bson.MongoTimestamp
var connTimeOut time.Duration

var chunkQueueLock sync.Mutex

var logFile *os.File
var logger *log.Logger
var oplogSyncTs map[string]bson.MongoTimestamp

type InitCollection struct {
	src           string
	dest          string
	srcDB         string
	srcColl       string
	srcUserName   string
	srcPassWord   string
	destDB        string
	destColl      string
	destUserName  string
	destPassWord  string
	srcClient     *mgo.Session
	destClient    *mgo.Session
	srcDBConn     *mgo.Database
	srcCollConn   *mgo.Collection
	destDBConn    *mgo.Database
	destCollConn  *mgo.Collection
	srcIsMongos   bool
	srcIsSharded  bool
	destIsMongos  bool
	srcOplogNodes map[string]string
	srcShardKey   bson.D
	srcChunks     []bson.M
}

func NewInitCollection(src, dest, srcDB, srcColl, srcUserName, srcPassWord, destDB, destColl, destUserName, destPassWord string) *InitCollection {
	i := &InitCollection{src, dest, srcDB, srcColl, srcUserName, srcPassWord, destDB, destColl, destUserName, destPassWord, nil, nil, nil, nil, nil, nil, false, false, false, nil, nil, nil}
	i.srcOplogNodes = make(map[string]string)
	return i
}

func (i *InitCollection) InitConn() {
	srcMongoUri := GenMongoDBUri(i.src, i.srcUserName, i.srcPassWord)
	destMongoUri := GenMongoDBUri(i.dest, i.destUserName, i.destPassWord)

	var err error

	i.srcClient, err = mgo.DialWithTimeout(srcMongoUri, connTimeOut)
	if err != nil {
		logger.Fatal("connect src failed.")
	} else {
		logger.Println("connect src success.")
	}

	i.destClient, err = mgo.DialWithTimeout(destMongoUri, connTimeOut)
	if err != nil {
		logger.Fatal("connect dest failed.")
	} else {
		logger.Println("connect src success.")
	}

	i.srcDBConn = i.srcClient.DB(i.srcDB)
	i.srcCollConn = i.srcDBConn.C(i.srcColl)

	i.destDBConn = i.destClient.DB(i.destDB)
	i.destCollConn = i.destDBConn.C(i.destColl)
}

func (i *InitCollection) GetSrcDestType() {
	command := bson.M{"isMaster": 1}
	result := bson.M{}
	i.srcDBConn.Run(command, &result)
	if result["msg"] == "isdbgrid" {
		i.srcIsMongos = true
		logger.Println("src is mongos")
	} else {
		logger.Println("src is not mongos,may be mongod.")
	}
	i.destDBConn.Run(command, &result)
	if result["msg"] == "isdbgrid" {
		i.destIsMongos = true
		logger.Println("dest is mongos")
	} else {
		logger.Println("dest is not mongos,may be mongod.")
	}
}

func (i *InitCollection) ShouldDoShardCollection() {
	if i.srcIsMongos && i.destIsMongos {
		command := bson.M{"collStats": i.srcColl}
		result := bson.M{}
		i.srcDBConn.Run(command, &result)
		srcIsSharded, _ := result["sharded"]
		if srcIsSharded == true {
			DoShardCollection = true
			i.srcIsSharded = true
			query := bson.M{"_id": i.srcDB + "." + i.srcColl}
			var result bson.D
			i.srcClient.DB("config").C("collections").Find(query).One(&result)
			for _, doc := range result {
				if doc.Name == "key" {
					if key, ok := doc.Value.(bson.D); ok {
						i.srcShardKey = key
						logger.Println("dest ns sharded,and shardkey is", i.srcShardKey)
						break
					}
				}
			}
		} else {
			logger.Println("dest ns not do shard as : src ns not sharded.")
		}
	} else {
		DoShardCollection = false
		logger.Println("dest ns not do shard as : src or dest is not mongos.")
	}
}

func (i *InitCollection) SelectOplogSyncNode(nodes string) string {
	var selectNode string
	var hosts string
	if strings.Contains(nodes, "/") {
		hosts = strings.Split(nodes, "/")[1]
	} else {
		hosts = nodes
	}

	var host string
	if strings.Contains(hosts, ",") {
		host = strings.Split(hosts, ",")[1]
	} else {
		host = hosts
	}

	mongoUri := GenMongoDBUri(host, i.srcUserName, i.srcPassWord)

	mongoClient, _ := mgo.Dial(mongoUri)

	var replConf, replStatus bson.M
	command := bson.M{"replSetGetStatus": 1}

	mongoClient.DB("local").C("system.replset").Find(bson.M{}).One(&replConf)
	mongoClient.DB("admin").Run(command, &replStatus)

	var isHostLegal bool

	if statusMembers, isStatusMembersLegal := replStatus["members"].([]interface{}); isStatusMembersLegal {
	selectNode:
		for _, statusMember := range statusMembers {
			if bsonStatusMember, isBsonStatusMember := statusMember.(bson.M); isBsonStatusMember {
				if bsonStatusMember["state"] == 1 || bsonStatusMember["state"] == 2 {
					if confMembers, isConfMembersLegal := replConf["members"].([]interface{}); isConfMembersLegal {
						for _, confMember := range confMembers {
							if bsonConfMember, isBsonConfMember := confMember.(bson.M); isBsonConfMember {
								if bsonConfMember["_id"] == bsonStatusMember["_id"] {
									if bsonConfMember["slaveDelay"] == 0 {
										if selectNode, isHostLegal = bsonConfMember["host"].(string); isHostLegal {
											selectNode = selectNode
										}
										if bsonStatusMember["state"] == 2 {
											break selectNode
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	mongoClient.Close()

	return selectNode

}

func (i *InitCollection) ShouldDoOplogSync() {
	if i.srcIsMongos {
		query := bson.M{}
		var result bson.M
		shardsColl := i.srcClient.DB("config").C("shards")
		shards := shardsColl.Find(query).Iter()
		var valueId, valueHost string
		var ok bool
		for shards.Next(&result) {
			if valueHost, ok = result["host"].(string); ok {
				if strings.Contains(valueHost, "/") {
					DoOplogSync = true
				}
				if valueId, ok = result["_id"].(string); ok {
					i.srcOplogNodes[valueId] = i.SelectOplogSyncNode(valueHost)
				}
			}
		}
	} else {
		count, _ := i.srcClient.DB("local").C("system.replset").Find(bson.M{}).Count()
		if count != 0 {
			DoOplogSync = true
			i.srcOplogNodes["shard"] = i.SelectOplogSyncNode(i.src)
		} else {
			DoOplogSync = false
		}
	}

	logger.Println("src oplog nodes:", i.srcOplogNodes)

	if DoOplogSync {
		logger.Println("do oplog sync as at least one replset deployed.")
	} else {
		logger.Println("will not do oplog sync as no replset found.")
	}

}

func (i *InitCollection) GetRandomShard() string {
	var randomShardId string
	srcShardsNum := len(i.srcOplogNodes)
	randomNum := rand.Intn(srcShardsNum)
	k := 0
	for shardId, _ := range i.srcOplogNodes {
		if randomNum == k {
			randomShardId = shardId
			break
		}
		k++
	}

	return randomShardId
}

func (i *InitCollection) SetStepSign() {
	i.GetSrcDestType()
	i.ShouldDoShardCollection()
	i.ShouldDoOplogSync()
}

func (i *InitCollection) ShardDestCollection() {
	logger.Println("start sharding dest collection")
	var result, command bson.M
	command = bson.M{"enableSharding": i.destDB}
	i.destClient.DB("admin").Run(command, &result)
	command = bson.M{"shardCollection": i.destDB + "." + i.destColl, "key": i.srcShardKey}
	err := i.destClient.DB("admin").Run(command, &result)
	if err != nil {
		logger.Fatal("shard dest collection fail,exit.")
	}
}

func (i *InitCollection) PreAllocChunks() {
	logger.Println("start pre split and move chunks")
	rand.Seed(time.Now().UnixNano())
	query := bson.M{"ns": i.srcDB + "." + i.srcColl}
	var command, result, chunk bson.M
	var randomShard string
	var chunkMin bson.M
	var isChunkLegal bool
	var err error
	srcChunksIter := i.srcClient.DB("config").C("chunks").Find(query).Iter()
	for srcChunksIter.Next(&chunk) {
		if chunkMin, isChunkLegal = chunk["min"].(bson.M); isChunkLegal {
			command = bson.M{"split": i.destDB + "." + i.destColl, "middle": chunkMin}
			err = i.destClient.DB("admin").Run(command, &result)
			if err != nil {
				logger.Println("split chunk fail,err is : ", err)
			}
			randomShard = i.GetRandomShard()
			command = bson.M{"moveChunk": i.srcDB + "." + i.srcColl, "find": chunkMin, "to": randomShard}
			err = i.destClient.DB("admin").Run(command, &result)
			if err != nil {
				logger.Println("move chunk to ", randomShard, " fail,err is : ", err)
			}
		}
		i.srcChunks = append(i.srcChunks, bson.M{"min": chunk["min"], "max": chunk["max"]})
	}
	logger.Println("pre split and move chunks finished.")
}

func (i *InitCollection) Run() {
	logger.Println("pre checking conn status.")
	i.InitConn()
	logger.Println("setting migrate step.")
	i.SetStepSign()
	if DoShardCollection {
		i.ShardDestCollection()
		i.PreAllocChunks()
	}
}

type CopyData struct {
	baseInitCollection *InitCollection
	workerNum          int
	queryKey           string
	queryChunk         []bson.M
}

func NewCopyData(i *InitCollection, findAndInsertWorkerNum int) *CopyData {
	return &CopyData{workerNum: findAndInsertWorkerNum, baseInitCollection: i}
}

func (c *CopyData) GetQueryKey() {
	if c.baseInitCollection.srcIsSharded {
		logger.Println("select the first key of shardkey as query condition.")
		c.queryKey = c.baseInitCollection.srcShardKey[0].Name
	} else {
		c.queryKey = " "
	}
	logger.Println("query condition key is : ", c.queryKey)
}

func (c *CopyData) RangeCopy(chunkQueue chan bson.M, ch chan int) {
	srcMongoUri := GenMongoDBUri(c.baseInitCollection.src, c.baseInitCollection.srcUserName, c.baseInitCollection.srcPassWord)
	destMongoUri := GenMongoDBUri(c.baseInitCollection.dest, c.baseInitCollection.destUserName, c.baseInitCollection.destPassWord)
	srcClient, _ := mgo.Dial(srcMongoUri)
	destClient, _ := mgo.Dial(destMongoUri)
	srcCollConn := srcClient.DB(c.baseInitCollection.srcDB).C(c.baseInitCollection.srcColl)
	destCollConn := destClient.DB(c.baseInitCollection.destDB).C(c.baseInitCollection.destColl)
	var query bson.M
	for {
		chunkQueueLock.Lock()
		if len(chunkQueue) == 0 {
			chunkQueueLock.Unlock()
			break
		} else {
			query = <-chunkQueue
			chunkQueueLock.Unlock()
		}
		documentsIter := srcCollConn.Find(query).Iter()
		var document bson.M
		for documentsIter.Next(&document) {
			destCollConn.Insert(document)
		}

	}
	ch <- 1
}

func (c *CopyData) StartCopyData() {
	chunkQueue := make(chan bson.M, len(c.queryChunk))
	tmpChunkFilter := make(map[interface{}]bool)
	for _, queryRange := range c.queryChunk {
		query := bson.M{c.queryKey: bson.M{"$gte": queryRange["min"], "$lt": queryRange["max"]}}
		if tmpChunkFilter[queryRange["min"]] == false {
			chunkQueue <- query
			tmpChunkFilter[queryRange["min"]] = true
		}
	}

	chs := make([]chan int, c.workerNum)

	for i := 0; i < c.workerNum; i++ {
		chs[i] = make(chan int)
		go c.RangeCopy(chunkQueue, chs[i])
	}
	for {
		logger.Println("chunk query queue has", len(c.queryChunk)-len(chunkQueue), "copying or copyed ,all chunk num is", len(c.queryChunk), "process is", (len(c.queryChunk)-len(chunkQueue))*100.0/len(c.queryChunk), "%")
		if len(chunkQueue) != 0 {
			time.Sleep(1e10)
		} else {
			logger.Println("all chunk copying,please wait before all copy goroutine finished.")
			break
		}
	}

	var finshGoRoutine int = 0

	for _, ch := range chs {
		<-ch
		finshGoRoutine++
		logger.Println("copy goroutine finished", finshGoRoutine, ",all goroutine num is", c.workerNum, "process is", finshGoRoutine*100.0/c.workerNum, "%")
	}

	logger.Println("copy data finished.")
}

func (c *CopyData) GetQueyRange() {
	if c.baseInitCollection.srcIsSharded {
		logger.Println("use chunk shardkey range getting query range")
		for _, chunk := range c.baseInitCollection.srcChunks {
			if minChunk, isMinChunkLegal := chunk["min"].(bson.M); isMinChunkLegal {
				if maxChunk, isMaxChunkLegal := chunk["max"].(bson.M); isMaxChunkLegal {
					minQueryKey := minChunk[c.queryKey]
					maxQueryKey := maxChunk[c.queryKey]
					c.queryChunk = append(c.queryChunk, bson.M{"min": minQueryKey, "max": maxQueryKey})
				}
			}
		}
	} else {
		c.queryChunk = append(c.queryChunk, bson.M{})
	}
	logger.Println("get query key range finished,multi conn copy data will be faster.")
}

func (c *CopyData) BuildIndexes() {
	logger.Println("start build indexes")
	indexes, _ := c.baseInitCollection.srcCollConn.Indexes()
	var err error
	for _, index := range indexes {
		err = c.baseInitCollection.destCollConn.EnsureIndex(index)
		if err != nil {
			logger.Println("build index Fail,please check it yourself.Fail index is : ", index)
		} else {
			logger.Println("build index : ", index, " success")
		}
	}
	logger.Println("build index fnished.")
}

func (c *CopyData) SaveLastOpTs() {
	logger.Println("save last ts in oplog,use it when syncing oplog.")
	chs := make([]chan bson.MongoTimestamp, len(c.baseInitCollection.srcOplogNodes))
	i := 0
	for _, oplogNode := range c.baseInitCollection.srcOplogNodes {
		chs[i] = make(chan bson.MongoTimestamp)
		go c.GetLastOpTs(chs[i], oplogNode)
		i++
	}

	for _, ts := range chs {
		tmpTs := <-ts
		if LastOpTs == 0 || tmpTs < LastOpTs {
			LastOpTs = tmpTs
		}
	}

	logger.Println("saved last op is : ", LastOpTs)
}

func (c *CopyData) GetLastOpTs(ch chan bson.MongoTimestamp, node string) {
	mongoUri := GenMongoDBUri(node, c.baseInitCollection.srcUserName, c.baseInitCollection.srcPassWord)
	mongoClient, _ := mgo.Dial(mongoUri)
	var result bson.M
	mongoClient.DB("local").C("oplog.rs").Find(bson.M{}).Sort("$natural").Limit(1).One(&result)
	var ts bson.MongoTimestamp
	if lastOpTs, ok := result["ts"].(bson.MongoTimestamp); ok {
		ts = lastOpTs
	}
	ch <- ts
}

func (c *CopyData) Run() {
	c.BuildIndexes()
	c.GetQueryKey()
	c.GetQueyRange()
	c.SaveLastOpTs()
	c.StartCopyData()
}

func (c *CopyData) LoadProgress() {

}

func (c *CopyData) DumpProgress() {

}

func (c *CopyData) DumpFailDocuments() {

}

type OplogSync struct {
	baseInitCollection *InitCollection
	oplogSyncWorkerNum int
}

func NewOplogSync(i *InitCollection, oplogSyncWorkerNum int) *OplogSync {
	return &OplogSync{i, oplogSyncWorkerNum}
}

func (o *OplogSync) ApplyOp(oplog bson.M) {
	op := oplog["op"]
	switch op {
	case "i":
		o.baseInitCollection.destCollConn.Insert(oplog["o"])
	case "u":
		o.baseInitCollection.destCollConn.Update(oplog["o2"], oplog["o"])
	case "d":
		o.baseInitCollection.destCollConn.Remove(oplog["o"])
	}
}

func (o *OplogSync) StartOplogSync(node string) {
	mongoUri := GenMongoDBUri(node, o.baseInitCollection.srcUserName, o.baseInitCollection.srcPassWord)
	mongoClient, _ := mgo.Dial(mongoUri)
	var result bson.M
	oplogIter := mongoClient.DB("local").C("oplog.rs").Find(bson.M{"ts": bson.M{"$gte": LastOpTs}, "ns": o.baseInitCollection.srcDB + "." + o.baseInitCollection.srcColl, "fromMigrate": bson.M{"$exists": false}}).Sort("$natural").LogReplay().Tail(-1)
	oplogSyncTs[node] = LastOpTs
	for oplogIter.Next(&result) {
		if ts, ok := result["ts"].(bson.MongoTimestamp); ok {
			oplogSyncTs[node] = ts
		}
		o.ApplyOp(result)
	}
}

func (o *OplogSync) Run() {
	for _, oplogNode := range o.baseInitCollection.srcOplogNodes {
		go o.StartOplogSync(oplogNode)
	}
	for {
		for node, ts := range oplogSyncTs {
			mongoClient, _ := mgo.Dial(GenMongoDBUri(node, o.baseInitCollection.srcUserName, o.baseInitCollection.srcPassWord))
			var firstOplog bson.M
			mongoClient.DB("local").C("oplog.rs").Find(bson.M{}).Sort("-$natural").Limit(1).One(&firstOplog)
			if firstOplogTs, ok := firstOplog["ts"].(bson.MongoTimestamp); ok {
				delay := firstOplogTs - ts
				logger.Println("node :", node, "delay is :", delay)
			}
			mongoClient.Close()

		}
		time.Sleep(1e9)
	}
}

func (o *OplogSync) LoadProgress() {

}

func (o *OplogSync) DumpProgress() {

}

func main() {
	var src, dest, srcDB, srcColl, destDB, destColl, srcUserName, srcPassWord, destUserName, destPassWord string
	var writeConcern, findAndInsertWorkerNum, oplogSyncWorkerNum int
	var journaled bool

	flag.StringVar(&src, "src", "127.0.0.1:27017", "src , ip:port")
	flag.StringVar(&dest, "dest", "127.0.0.1:27017", "dest , ip:port")

	flag.StringVar(&srcDB, "srcDB", "test", "db of src being migrated")
	flag.StringVar(&srcColl, "srcColl", "test", "collection of src being migrated")
	flag.StringVar(&srcUserName, "srcUserName", "", "src auth user name")
	flag.StringVar(&srcPassWord, "srcPassWord", "", "src auth password")

	flag.StringVar(&destDB, "destDB", "test", "db of dest being migrated")
	flag.StringVar(&destColl, "destColl", "test", "collection of dest being migrated")
	flag.StringVar(&destUserName, "destUserName", "", "dest auth user name")
	flag.StringVar(&destPassWord, "destPassWord", "", "dest auth password")

	flag.IntVar(&writeConcern, "w", 1, "write acknowledged")
	flag.BoolVar(&journaled, "j", true, "journaled")

	flag.IntVar(&findAndInsertWorkerNum, "findAndInsertWorkerNum", 10, "find and insert worker num")
	flag.IntVar(&oplogSyncWorkerNum, "oplogSyncWorkerNum", 10, "oplog sync worker num")

	flag.Parse()

	logFile, _ = os.OpenFile("log/ms.log", os.O_RDWR|os.O_CREATE, 0777)
	logger = log.New(logFile, "\r\n", log.Ldate|log.Ltime|log.Lshortfile)
	connTimeOut = time.Duration(5) * time.Second
	oplogSyncTs = make(map[string]bson.MongoTimestamp)

	//init step
	logger.Println("start init collection")
	initCollection := NewInitCollection(src, dest, srcDB, srcColl, srcUserName, srcPassWord, destDB, destColl, destUserName, destPassWord)
	initCollection.Run()

	//copy data step

	logger.Println("start copy data")
	copyData := NewCopyData(initCollection, findAndInsertWorkerNum)
	copyData.Run()

	//oplog sync step

	logger.Println("start sync oplog")
	oplogSync := NewOplogSync(initCollection, oplogSyncWorkerNum)
	oplogSync.Run()

}
