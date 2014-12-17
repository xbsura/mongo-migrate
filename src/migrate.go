package main

import (
	"flag"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

//generate mongodb uri , use ip,port,username and password
func GenMongoDBUri(addr, userName, passWord string) string {
	var mongoDBUri string
	if userName == "" || passWord == "" {
		mongoDBUri = "mongodb://" + addr
	} else {
		mongoDBUri = "mongodb://" + userName + ":" + passWord + "@" + addr
	}
	return mongoDBUri
}

var DoShardCollection bool // if true,do shard and pre split,movechunk in dest mongos
var DoOplogSync bool       // if true,apply oplog during copy data
var connTimeOut time.Duration

var chunkQueueLock sync.Mutex // channel does not have timeout or getNoWait,use this lock,check whether empty first before get.

var logFile *os.File
var logger *log.Logger
var oplogSyncTs map[string]bson.MongoTimestamp // while syncing oplog,each replset syncing ts

type InitCollection struct {
	src                 string
	dest                string
	srcDB               string
	srcColl             string
	srcUserName         string
	srcPassWord         string
	destDB              string
	destColl            string
	destUserName        string
	destPassWord        string
	writeAck            int
	writeMode           string
	journal             bool
	fsync               bool
	srcClient           *mgo.Session
	destClient          *mgo.Session
	srcDBConn           *mgo.Database
	srcCollConn         *mgo.Collection
	destDBConn          *mgo.Database
	destCollConn        *mgo.Collection
	srcIsMongos         bool
	srcIsSharded        bool
	destIsMongos        bool
	srcOplogNodes       map[string]string
	srcShardKey         bson.D
	srcChunks           []bson.M
	srcBalancerStopped  bool
	destBalancerStopped bool
}

func NewInitCollection(src, dest, srcDB, srcColl, srcUserName, srcPassWord, destDB, destColl, destUserName, destPassWord string, writeAck int, writeMode string, fsync, journal bool) *InitCollection {
	initCollection := &InitCollection{src, dest, srcDB, srcColl, srcUserName, srcPassWord, destDB, destColl, destUserName, destPassWord, writeAck, writeMode, journal, fsync, nil, nil, nil, nil, nil, nil, false, false, false, nil, nil, nil, false, false}
	initCollection.srcOplogNodes = make(map[string]string)
	return initCollection
}

func (initCollection *InitCollection) InitConn() {
	srcMongoUri := GenMongoDBUri(initCollection.src, initCollection.srcUserName, initCollection.srcPassWord)
	destMongoUri := GenMongoDBUri(initCollection.dest, initCollection.destUserName, initCollection.destPassWord)

	var err error

	initCollection.srcClient, err = mgo.DialWithTimeout(srcMongoUri, connTimeOut)
	if err != nil {
		logger.Panicln("connect src failed.")
	} else {
		logger.Println("connect src success.")
	}

	initCollection.destClient, err = mgo.DialWithTimeout(destMongoUri, connTimeOut)
	if err != nil {
		logger.Panicln("connect dest failed.")
	} else {
		logger.Println("connect src success.")
	}

	initCollection.srcDBConn = initCollection.srcClient.DB(initCollection.srcDB)
	initCollection.srcCollConn = initCollection.srcDBConn.C(initCollection.srcColl)

	initCollection.destDBConn = initCollection.destClient.DB(initCollection.destDB)
	initCollection.destCollConn = initCollection.destDBConn.C(initCollection.destColl)
}

func (initCollection *InitCollection) GetSrcDestType() {
	command := bson.M{"isMaster": 1}
	result := bson.M{}
	initCollection.srcDBConn.Run(command, &result)
	if result["msg"] == "isdbgrid" {
		initCollection.srcIsMongos = true
		logger.Println("src is mongos")
	} else {
		logger.Println("src is not mongos,may be mongod.")
	}
	initCollection.destDBConn.Run(command, &result)
	if result["msg"] == "isdbgrid" {
		initCollection.destIsMongos = true
		logger.Println("dest is mongos")
	} else {
		logger.Println("dest is not mongos,may be mongod.")
	}
}

//only src and dest are ALL mongos AND src ns sharded,then do shard on dest ns
func (initCollection *InitCollection) ShouldDoShardCollection() {
	if initCollection.srcIsMongos && initCollection.destIsMongos {
		command := bson.M{"collStats": initCollection.srcColl}
		result := bson.M{}
		initCollection.srcDBConn.Run(command, &result)
		srcIsSharded, _ := result["sharded"]
		if srcIsSharded == true {
			DoShardCollection = true
			initCollection.srcIsSharded = true
			query := bson.M{"_id": initCollection.srcDB + "." + initCollection.srcColl}
			var result bson.D
			initCollection.srcClient.DB("config").C("collections").Find(query).One(&result)
			for _, doc := range result {
				if doc.Name == "key" {
					if key, ok := doc.Value.(bson.D); ok {
						initCollection.srcShardKey = key
						logger.Println("dest ns sharded,and shardkey is", initCollection.srcShardKey)
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

// select a node which has no slaveDelay and better to be secondary if possible
func (initCollection *InitCollection) SelectOplogSyncNode(nodes string) string {
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

	mongoUri := GenMongoDBUri(host, initCollection.srcUserName, initCollection.srcPassWord)

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

// if one replset available to src ns,do oplog sync
func (initCollection *InitCollection) ShouldDoOplogSync() {
	var query bson.M
	var result bson.M
	if initCollection.srcIsMongos {
		if initCollection.srcIsSharded {
			query = bson.M{}
		} else {
			initCollection.srcClient.DB("config").C("databases").Find(bson.M{"_id": initCollection.srcDB}).One(&result)
			primaryShard := result["primary"]
			query = bson.M{"_id": primaryShard}
		}
		logger.Println("src ns do oplogsync chenck for query", query)
		shardsColl := initCollection.srcClient.DB("config").C("shards")
		shards := shardsColl.Find(query).Iter()
		var valueId, valueHost string
		var ok bool
		for shards.Next(&result) {
			if valueHost, ok = result["host"].(string); ok {
				if strings.Contains(valueHost, "/") {
					DoOplogSync = true
				}
				if valueId, ok = result["_id"].(string); ok {
					initCollection.srcOplogNodes[valueId] = initCollection.SelectOplogSyncNode(valueHost)
				}
			}
		}
	} else {
		count, _ := initCollection.srcClient.DB("local").C("system.replset").Find(bson.M{}).Count()
		if count != 0 {
			DoOplogSync = true
			initCollection.srcOplogNodes["shard"] = initCollection.SelectOplogSyncNode(initCollection.src)
		} else {
			DoOplogSync = false
		}
	}

	logger.Println("src oplog nodes:", initCollection.srcOplogNodes)

	if DoOplogSync {
		logger.Println("do oplog sync as at least one replset deployed.")
	} else {
		logger.Println("will not do oplog sync as no replset found.")
	}

}

// when moveing chunk,select a random shard name
func (initCollection *InitCollection) GetRandomShard() string {
	var randomShardId string
	srcShardsNum := len(initCollection.srcOplogNodes)
	randomNum := rand.Intn(srcShardsNum)
	k := 0
	for shardId, _ := range initCollection.srcOplogNodes {
		if randomNum == k {
			randomShardId = shardId
			break
		}
		k++
	}

	return randomShardId
}

func (initCollection *InitCollection) SetStepSign() {
	initCollection.GetSrcDestType()
	initCollection.ShouldDoShardCollection()
	initCollection.ShouldDoOplogSync()
}

// do shard on dest ns
func (initCollection *InitCollection) ShardDestCollection() {
	logger.Println("start sharding dest collection")
	var result, command bson.D
	command = bson.D{{"enableSharding", initCollection.destDB}}
	initCollection.destClient.DB("admin").Run(command, &result)
	command = bson.D{{"shardCollection", initCollection.destDB + "." + initCollection.destColl}, {"key", initCollection.srcShardKey}}
	err := initCollection.destClient.DB("admin").Run(command, &result)
	if err != nil {
		logger.Panicln("shard dest collection fail:", err)
	}
}

// pre split and move chunks
func (initCollection *InitCollection) PreAllocChunks() {
	logger.Println("start pre split and move chunks")
	rand.Seed(time.Now().UnixNano())
	query := bson.M{"ns": initCollection.srcDB + "." + initCollection.srcColl}
	var result, chunk bson.M
	var command bson.D
	var randomShard string
	var chunkMin bson.M
	var isChunkLegal bool
	var err error
	srcChunksIter := initCollection.srcClient.DB("config").C("chunks").Find(query).Iter()
	for srcChunksIter.Next(&chunk) {
		if chunkMin, isChunkLegal = chunk["min"].(bson.M); isChunkLegal {
			command = bson.D{{"split", initCollection.destDB + "." + initCollection.destColl}, {"middle", chunkMin}}
			err = initCollection.destClient.DB("admin").Run(command, &result)
			if err != nil {
				logger.Println("split chunk fail,err is : ", err)
			} else {
				logger.Println("split chunk success")
			}
			randomShard = initCollection.GetRandomShard()
			command = bson.D{{"moveChunk", initCollection.srcDB + "." + initCollection.srcColl}, {"find", chunkMin}, {"to", randomShard}}
			err = initCollection.destClient.DB("admin").Run(command, &result)
			if err != nil {
				logger.Println("move chunk to ", randomShard, " fail,err is : ", err)
			} else {
				logger.Println("move chunk to ", randomShard, "success")
			}
		}
		initCollection.srcChunks = append(initCollection.srcChunks, bson.M{"min": chunk["min"], "max": chunk["max"]})
	}
	logger.Println("pre split and move chunks finished.")
}

func (initCollection *InitCollection) StopBalancer() {
	query := bson.M{"_id": "balancer"}
	updateDocument := bson.M{"$set": bson.M{"stopped": true}}
	var result bson.M
	if initCollection.srcIsMongos {
		initCollection.srcClient.DB("config").C("settings").Find(query).One(&result)
		if srcBalancerStopped, srcBalancerStateChanged := result["stopped"].(bool); srcBalancerStateChanged {
			initCollection.srcBalancerStopped = srcBalancerStopped
		} else {
			initCollection.srcBalancerStopped = false
		}

	}
	if initCollection.destIsMongos {
		initCollection.destClient.DB("config").C("settings").Find(query).One(&result)
		if destBalancerStopped, destBalancerStateChanged := result["stopped"].(bool); destBalancerStateChanged {
			initCollection.destBalancerStopped = destBalancerStopped
		} else {
			initCollection.destBalancerStopped = false
		}
	}
	logger.Println("stop src balancer ... ")
	initCollection.destClient.DB("config").C("settings").Update(query, updateDocument)

	logger.Println("stop dest balancer...")
	initCollection.srcClient.DB("config").C("settings").Update(query, updateDocument)

	logger.Println("src origin balancer is stopped:", initCollection.srcBalancerStopped)
	logger.Println("dest origin balancer is stopped:", initCollection.destBalancerStopped)
}

func (initCollection *InitCollection) ResetBalancer() {
	query := bson.M{"_id": "balancer"}
	if initCollection.srcIsMongos {
		initCollection.srcClient.DB("config").C("settings").Update(query, bson.M{"stopped": initCollection.srcBalancerStopped})
	}
	if initCollection.destIsMongos {
		initCollection.destClient.DB("config").C("settings").Update(query, bson.M{"stopped": initCollection.destBalancerStopped})
	}
}

func (initCollection *InitCollection) Run() {
	logger.Println("pre checking conn status.")
	initCollection.InitConn()
	logger.Println("setting migrate step.")
	initCollection.SetStepSign()
	initCollection.StopBalancer()
	if DoShardCollection {
		initCollection.ShardDestCollection()
		initCollection.PreAllocChunks()
	}
}

type CopyData struct {
	initCollection *InitCollection
	workerNum      int
	queryKey       string
	queryChunk     []bson.M
}

func NewCopyData(initCollection *InitCollection, findAndInsertWorkerNum int) *CopyData {
	return &CopyData{workerNum: findAndInsertWorkerNum, initCollection: initCollection}
}

// when doing find and insert , we use many goroutine , each goroutine copy one range of data,
// we use the first key of shardkey as the query condition
func (copyData *CopyData) GetQueryKey() {
	if copyData.initCollection.srcIsSharded {
		logger.Println("select the first key of shardkey as query condition.")
		copyData.queryKey = copyData.initCollection.srcShardKey[0].Name
	} else {
		copyData.queryKey = " "
	}
	logger.Println("query condition key is : ", copyData.queryKey)
}

// one goroutine,find and insert data
func (copyData *CopyData) RangeCopy(chunkQueue chan bson.M, ch chan int) {
	srcMongoUri := GenMongoDBUri(copyData.initCollection.src, copyData.initCollection.srcUserName, copyData.initCollection.srcPassWord)
	destMongoUri := GenMongoDBUri(copyData.initCollection.dest, copyData.initCollection.destUserName, copyData.initCollection.destPassWord)
	srcClient, _ := mgo.Dial(srcMongoUri)
	destClient, _ := mgo.Dial(destMongoUri)
	destClient.EnsureSafe(&mgo.Safe{W: copyData.initCollection.writeAck, WMode: copyData.initCollection.writeMode, FSync: copyData.initCollection.fsync, J: copyData.initCollection.journal})
	srcCollConn := srcClient.DB(copyData.initCollection.srcDB).C(copyData.initCollection.srcColl)
	destCollConn := destClient.DB(copyData.initCollection.destDB).C(copyData.initCollection.destColl)
	var query bson.M
	for {
		chunkQueueLock.Lock() // as read from channel has no wait time or no wait get,we use one lock here,check whether the queue is empty
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

// make query queue,start copy goroutine
func (copyData *CopyData) StartCopyData() {
	chunkQueue := make(chan bson.M, len(copyData.queryChunk))
	tmpChunkFilter := make(map[interface{}]bool) // in case copy same chunk(as _id is unique,will not cause error,but waste time)
	for _, queryRange := range copyData.queryChunk {
		query := bson.M{copyData.queryKey: bson.M{"$gte": queryRange["min"], "$lt": queryRange["max"]}}
		if tmpChunkFilter[queryRange["min"]] == false {
			chunkQueue <- query
			tmpChunkFilter[queryRange["min"]] = true
		}
	}

	chs := make([]chan int, copyData.workerNum)

	for i := 0; i < copyData.workerNum; i++ {
		chs[i] = make(chan int)
		go copyData.RangeCopy(chunkQueue, chs[i])
	}
	for {
		logger.Println("chunk query queue has", len(copyData.queryChunk)-len(chunkQueue), "copying or copyed ,all chunk num is", len(copyData.queryChunk), "process is", (len(copyData.queryChunk)-len(chunkQueue))*100.0/len(copyData.queryChunk), "%")
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
		logger.Println("copy goroutine finished", finshGoRoutine, ",all goroutine num is", copyData.workerNum, "process is", finshGoRoutine*100.0/copyData.workerNum, "%")
	}

	logger.Println("copy data finished.")
}

func (copyData *CopyData) GetQueyRange() {
	if copyData.initCollection.srcIsSharded {
		logger.Println("use chunk shardkey range getting query range")
		for _, chunk := range copyData.initCollection.srcChunks {
			if minChunk, isMinChunkLegal := chunk["min"].(bson.M); isMinChunkLegal {
				if maxChunk, isMaxChunkLegal := chunk["max"].(bson.M); isMaxChunkLegal {
					minQueryKey := minChunk[copyData.queryKey]
					maxQueryKey := maxChunk[copyData.queryKey]
					copyData.queryChunk = append(copyData.queryChunk, bson.M{"min": minQueryKey, "max": maxQueryKey})
				}
			}
		}
	} else {
		copyData.queryChunk = append(copyData.queryChunk, bson.M{})
	}
	logger.Println("get query key range finished,multi conn copy data will be faster.")
}

// *bug:* text index create fail.
func (copyData *CopyData) BuildIndexes() {
	logger.Println("start build indexes")
	indexes, _ := copyData.initCollection.srcCollConn.Indexes()
	var err error
	for _, index := range indexes {
		err = copyData.initCollection.destCollConn.EnsureIndex(index)
		if err != nil {
			logger.Println("build index Fail,please check it yourself.Fail index is : ", index)
		} else {
			logger.Println("build index : ", index, " success")
		}
	}
	logger.Println("build index fnished.")
}

// find the smallest ts and save it(as oplog can be ran many times).
func (copyData *CopyData) SaveLastOpTs() {
	logger.Println("save last ts in oplog,use it when syncing oplog.")
	chs := make([]chan int, len(copyData.initCollection.srcOplogNodes))
	i := 0
	for shard, oplogNode := range copyData.initCollection.srcOplogNodes {
		chs[i] = make(chan int)
		go copyData.GetLastOpTs(chs[i], shard, oplogNode)
		i++
	}

	for _, ts := range chs {
		<-ts
	}

	logger.Println("saved last op ts is : ", oplogSyncTs)
}

func (copyData *CopyData) GetLastOpTs(ch chan int, shard string, node string) {
	mongoUri := GenMongoDBUri(node, copyData.initCollection.srcUserName, copyData.initCollection.srcPassWord)
	mongoClient, _ := mgo.Dial(mongoUri)
	var result bson.M
	mongoClient.DB("local").C("oplog.rs").Find(bson.M{}).Sort("-$natural").Limit(1).One(&result)
	if lastOpTs, ok := result["ts"].(bson.MongoTimestamp); ok {
		oplogSyncTs[shard+"/"+node] = lastOpTs
	}
	ch <- 1
}

func (copyData *CopyData) Run() {
	copyData.BuildIndexes()
	copyData.GetQueryKey()
	copyData.GetQueyRange()
	copyData.SaveLastOpTs()
	copyData.StartCopyData()
}

func (copyData *CopyData) LoadProgress() {

}

func (copyData *CopyData) DumpProgress() {

}

func (copyData *CopyData) DumpFailDocuments() {

}

type OplogSync struct {
	initCollection *InitCollection
}

func NewOplogSync(initCollection *InitCollection) *OplogSync {
	return &OplogSync{initCollection}
}

func (oplogSync *OplogSync) ApplyOp(oplog bson.M) {
	op := oplog["op"]
	switch op {
	case "i":
		oplogSync.initCollection.destCollConn.Insert(oplog["o"])
	case "u":
		oplogSync.initCollection.destCollConn.Update(oplog["o2"], oplog["o"])
	case "d":
		oplogSync.initCollection.destCollConn.Remove(oplog["o"])
	}
}

func (oplogSync *OplogSync) StartOplogSync(shard string, node string) {
	mongoUri := GenMongoDBUri(node, oplogSync.initCollection.srcUserName, oplogSync.initCollection.srcPassWord)
	mongoClient, _ := mgo.Dial(mongoUri)
	mongoClient.EnsureSafe(&mgo.Safe{W: oplogSync.initCollection.writeAck, WMode: oplogSync.initCollection.writeMode, FSync: oplogSync.initCollection.fsync, J: oplogSync.initCollection.journal})
	var result bson.M

	// ok to use OPLOG_REPLAY here now
	startLastOpTs := oplogSyncTs[shard+"/"+node]
	oplogIter := mongoClient.DB("local").C("oplog.rs").Find(bson.M{"ts": bson.M{"$gte": startLastOpTs}}).LogReplay().Sort("$natural").Tail(-1)
	for oplogIter.Next(&result) {
		if ts, ok := result["ts"].(bson.MongoTimestamp); ok {
			oplogSyncTs[shard+"/"+node] = ts
		}

		if result["fromMigrate"] == true {
			continue
		}

		if result["ns"] != oplogSync.initCollection.srcDB+"."+oplogSync.initCollection.srcColl {
			continue
		}

		oplogSync.ApplyOp(result)
	}
}

func (oplogSync *OplogSync) Run() {
	isResetBalancer := false
	for shard, oplogNode := range oplogSync.initCollection.srcOplogNodes {
		go oplogSync.StartOplogSync(shard, oplogNode)
	}
	for {
		shouldResetBalancer := true
		for shardAndNode, ts := range oplogSyncTs {
			node := strings.Split(shardAndNode, "/")[1]
			mongoClient, _ := mgo.Dial(GenMongoDBUri(node, oplogSync.initCollection.srcUserName, oplogSync.initCollection.srcPassWord))
			var firstOplog bson.M
			mongoClient.DB("local").C("oplog.rs").Find(bson.M{}).Sort("-$natural").Limit(1).One(&firstOplog)
			if firstOplogTs, ok := firstOplog["ts"].(bson.MongoTimestamp); ok {
				delay := firstOplogTs>>32 - ts>>32
				logger.Print("node :", shardAndNode, " delay is :", delay)
				if delay > 100 {
					shouldResetBalancer = false
				}
			}
			mongoClient.Close()
		}
		if shouldResetBalancer && !isResetBalancer {
			logger.Println("oplog sync almost finished,reset balancer state...")
			oplogSync.initCollection.ResetBalancer()
			isResetBalancer = true
		}
		time.Sleep(5e9)
	}
}

func (oplogSync *OplogSync) LoadProgress() {

}

func (oplogSync *OplogSync) DumpProgress() {

}

func main() {
	var src, dest, srcDB, srcColl, destDB, destColl, srcUserName, srcPassWord, destUserName, destPassWord string
	var findAndInsertWorkerNum int

	var writeAck int
	var writeMode string
	var journal, fsync bool

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

	flag.IntVar(&writeAck, "writeAck", 1, "write acknowledged")
	flag.StringVar(&writeMode, "write Mode, e.g Majority", "", "write acknowledged")
	flag.BoolVar(&journal, "journal", true, "whether use journal")
	flag.BoolVar(&fsync, "fsync", false, "whether use fsync for each write")

	flag.IntVar(&findAndInsertWorkerNum, "findAndInsertWorkerNum", 10, "find and insert worker num")

	flag.Parse()

	logFile, _ = os.OpenFile("log/ms.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	writers := []io.Writer{
		logFile,
		os.Stdout,
	}
	fileAndStdoutWriter := io.MultiWriter(writers...)
	logger = log.New(fileAndStdoutWriter, "\r\n", log.Ldate|log.Ltime|log.Lshortfile)
	connTimeOut = time.Duration(5) * time.Second
	oplogSyncTs = make(map[string]bson.MongoTimestamp)

	defer func() {
		logger.Println("============================end this job=================================")
		logFile.Close()
	}()
	logger.Println("===================================start one new job.==================================")
	//init step
	logger.Println("start init collection")
	initCollection := NewInitCollection(src, dest, srcDB, srcColl, srcUserName, srcPassWord, destDB, destColl, destUserName, destPassWord, writeAck, writeMode, journal, fsync)
	initCollection.Run()

	//copy data step

	logger.Println("start copy data")
	copyData := NewCopyData(initCollection, findAndInsertWorkerNum)
	copyData.Run()

	//oplog sync step

	logger.Println("start sync oplog")
	oplogSync := NewOplogSync(initCollection)
	oplogSync.Run()

}
