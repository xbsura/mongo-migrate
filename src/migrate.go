package main

import (
	"flag"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
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
	initColl := &InitCollection{src, dest, srcDB, srcColl, srcUserName, srcPassWord, destDB, destColl, destUserName, destPassWord, writeAck, writeMode, journal, fsync, nil, nil, nil, nil, nil, nil, false, false, false, nil, nil, nil, false, false}
	initColl.srcOplogNodes = make(map[string]string)
	return initColl
}

func (initColl *InitCollection) InitConn() {
	srcMongoUri := GenMongoDBUri(initColl.src, initColl.srcUserName, initColl.srcPassWord)
	destMongoUri := GenMongoDBUri(initColl.dest, initColl.destUserName, initColl.destPassWord)

	var err error

	initColl.srcClient, err = mgo.DialWithTimeout(srcMongoUri, connTimeOut)
	if err != nil {
		logger.Panicln("connect src failed.")
	} else {
		logger.Println("connect src success.")
	}

	initColl.destClient, err = mgo.DialWithTimeout(destMongoUri, connTimeOut)
	if err != nil {
		logger.Panicln("connect dest failed.")
	} else {
		logger.Println("connect src success.")
	}

	initColl.srcDBConn = initColl.srcClient.DB(initColl.srcDB)
	initColl.srcCollConn = initColl.srcDBConn.C(initColl.srcColl)

	initColl.destDBConn = initColl.destClient.DB(initColl.destDB)
	initColl.destCollConn = initColl.destDBConn.C(initColl.destColl)
}

func (initColl *InitCollection) GetSrcDestType() {
	command := bson.M{"isMaster": 1}
	result := bson.M{}
	initColl.srcDBConn.Run(command, &result)
	if result["msg"] == "isdbgrid" {
		initColl.srcIsMongos = true
		logger.Println("src is mongos")
	} else {
		logger.Println("src is not mongos,may be mongod.")
	}
	initColl.destDBConn.Run(command, &result)
	if result["msg"] == "isdbgrid" {
		initColl.destIsMongos = true
		logger.Println("dest is mongos")
	} else {
		logger.Println("dest is not mongos,may be mongod.")
	}
}

//only src and dest are ALL mongos AND src ns sharded,then do shard on dest ns
func (initColl *InitCollection) ShouldDoShardCollection() {
	if initColl.srcIsMongos && initColl.destIsMongos {
		command := bson.M{"collStats": initColl.srcColl}
		result := bson.M{}
		initColl.srcDBConn.Run(command, &result)
		srcIsSharded, _ := result["sharded"]
		if srcIsSharded == true {
			DoShardCollection = true
			initColl.srcIsSharded = true
			query := bson.M{"_id": initColl.srcDB + "." + initColl.srcColl}
			var result bson.D
			initColl.srcClient.DB("config").C("collections").Find(query).One(&result)
			for _, doc := range result {
				if doc.Name == "key" {
					if key, ok := doc.Value.(bson.D); ok {
						initColl.srcShardKey = key
						logger.Println("dest ns sharded,and shardkey is", initColl.srcShardKey)
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
func (initColl *InitCollection) SelectOplogSyncNode(nodes string) string {
	var selectHost string
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

	mongoUri := GenMongoDBUri(host, initColl.srcUserName, initColl.srcPassWord)

	mongoClient, _ := mgo.Dial(mongoUri)

	var replConf, replStatus bson.M
	command := bson.M{"replSetGetStatus": 1}

	replConfMap := make(map[interface{}]bson.M)
	replStatusMap := make(map[interface{}]interface{})

	mongoClient.DB("local").C("system.replset").Find(bson.M{}).One(&replConf)
	mongoClient.DB("admin").Run(command, &replStatus)

	if confMembers, isConfMembersLegal := replConf["members"].([]interface{}); isConfMembersLegal {
		for _, confMember := range confMembers {
			if bsonConfMember, isBsonConfMember := confMember.(bson.M); isBsonConfMember {
				hostAndDelay := bson.M{"host": bsonConfMember["host"], "slaveDelay": bsonConfMember["slaveDelay"]}
				replConfMap[bsonConfMember["_id"]] = hostAndDelay
			}
		}
	}

	if statusMembers, isStatusMembersLegal := replStatus["members"].([]interface{}); isStatusMembersLegal {
		for _, statusMember := range statusMembers {
			if bsonStatusMember, isBsonStatusMember := statusMember.(bson.M); isBsonStatusMember {
				replStatusMap[bsonStatusMember["_id"]] = bsonStatusMember["state"]
			}
		}
	}

	logger.Println("replStatus:", replStatusMap)

	logger.Println("replConf:", replConfMap)

	for id, state := range replStatusMap {
		if state == 1 || state == 2 {
			if replConfMap[id]["slaveDelay"] == 0 {
				if host, ok := replConfMap[id]["host"].(string); ok {
					logger.Println("one node selected")
					selectHost = host
				}
			}
			if state == 2 {
				break
			}
		}
	}

	logger.Println("oplog sync node selected:", selectHost)

	mongoClient.Close()

	return selectHost

}

// if one replset available to src ns,do oplog sync
func (initColl *InitCollection) ShouldDoOplogSync() {
	var query bson.M
	var result bson.M
	if initColl.srcIsMongos {
		if initColl.srcIsSharded {
			query = bson.M{}
		} else {
			databaseDB := initColl.srcClient.DB("config").C("databases")
			databaseDB.Find(bson.M{"_id": initColl.srcDB}).One(&result)
			primaryShard := result["primary"]
			query = bson.M{"_id": primaryShard}
		}
		logger.Println("src ns do oplogsync chenck for query", query)
		shardsColl := initColl.srcClient.DB("config").C("shards")
		shards := shardsColl.Find(query).Iter()
		var valueId, valueHost string
		var ok bool
		for shards.Next(&result) {
			if valueHost, ok = result["host"].(string); ok {
				if strings.Contains(valueHost, "/") {
					DoOplogSync = true
				}
				if valueId, ok = result["_id"].(string); ok {
					initColl.srcOplogNodes[valueId] = initColl.SelectOplogSyncNode(valueHost)
				}
			}
		}
	} else {
		count, _ := initColl.srcClient.DB("local").C("system.replset").Find(bson.M{}).Count()
		if count != 0 {
			DoOplogSync = true
			initColl.srcOplogNodes["shard"] = initColl.SelectOplogSyncNode(initColl.src)
		} else {
			DoOplogSync = false
		}
	}

	logger.Println("src oplog nodes:", initColl.srcOplogNodes)

	if DoOplogSync {
		logger.Println("do oplog sync as at least one replset deployed.")
	} else {
		logger.Println("will not do oplog sync as no replset found.")
	}

}

// when moveing chunk,select a random shard name
func (initColl *InitCollection) GetRandomShard() string {
	var randomShardId string
	srcShardsNum := len(initColl.srcOplogNodes)
	randomNum := rand.Intn(srcShardsNum)
	k := 0
	for shardId, _ := range initColl.srcOplogNodes {
		if randomNum == k {
			randomShardId = shardId
			break
		}
		k++
	}

	return randomShardId
}

func (initColl *InitCollection) SetStepSign() {
	initColl.GetSrcDestType()
	initColl.ShouldDoShardCollection()
	initColl.ShouldDoOplogSync()
}

// do shard on dest ns
func (initColl *InitCollection) ShardDestCollection() {
	destNs := initColl.destDB + "." + initColl.destColl
	logger.Println("start sharding dest collection")
	var result, command bson.D
	command = bson.D{{"enableSharding", initColl.destDB}}
	initColl.destClient.DB("admin").Run(command, &result)
	command = bson.D{{"shardCollection", destNs}, {"key", initColl.srcShardKey}}
	err := initColl.destClient.DB("admin").Run(command, &result)
	if err != nil {
		logger.Panicln("shard dest collection fail:", err)
	}
}

// pre split and move chunks
func (initColl *InitCollection) PreAllocChunks() {
	logger.Println("start pre split and move chunks")
	rand.Seed(time.Now().UnixNano())
	query := bson.M{"ns": initColl.srcDB + "." + initColl.srcColl}
	var result, chunk bson.M
	var command bson.D
	var randomShard string
	var chunkMin bson.M
	var isChunkLegal bool
	var err error
	destNs := initColl.destDB + "." + initColl.destColl
	srcChunksIter := initColl.srcClient.DB("config").C("chunks").Find(query).Iter()
	for srcChunksIter.Next(&chunk) {
		if chunkMin, isChunkLegal = chunk["min"].(bson.M); isChunkLegal {
			command = bson.D{{"split", destNs}, {"middle", chunkMin}}
			err = initColl.destClient.DB("admin").Run(command, &result)
			if err != nil {
				logger.Println("split chunk fail,err is : ", err)
			} else {
				logger.Println("split chunk success")
			}
			randomShard = initColl.GetRandomShard()
			command = bson.D{{"moveChunk", destNs}, {"find", chunkMin}, {"to", randomShard}}
			err = initColl.destClient.DB("admin").Run(command, &result)
			if err != nil {
				logger.Println("move chunk to ", randomShard, " fail,err is : ", err)
			} else {
				logger.Println("move chunk to ", randomShard, "success")
			}
		}
		initColl.srcChunks = append(initColl.srcChunks, bson.M{"min": chunk["min"], "max": chunk["max"]})
	}
	logger.Println("pre split and move chunks finished.")
}

func (initColl *InitCollection) StopBalancer() {
	query := bson.M{"_id": "balancer"}
	updateDocument := bson.M{"$set": bson.M{"stopped": true}}
	var result bson.M
	var srcBalancerStopped, destBalancerStopped, ok bool
	if initColl.srcIsMongos {
		initColl.srcClient.DB("config").C("settings").Find(query).One(&result)
		if srcBalancerStopped, ok = result["stopped"].(bool); ok {
			initColl.srcBalancerStopped = srcBalancerStopped
		} else {
			initColl.srcBalancerStopped = false
		}

	}
	if initColl.destIsMongos {
		initColl.destClient.DB("config").C("settings").Find(query).One(&result)
		if destBalancerStopped, ok = result["stopped"].(bool); ok {
			initColl.destBalancerStopped = destBalancerStopped
		} else {
			initColl.destBalancerStopped = false
		}
	}
	logger.Println("stop src balancer ... ")
	initColl.destClient.DB("config").C("settings").Update(query, updateDocument)

	logger.Println("stop dest balancer...")
	initColl.srcClient.DB("config").C("settings").Update(query, updateDocument)

	logger.Println("src origin balancer is stopped:", initColl.srcBalancerStopped)
	logger.Println("dest origin balancer is stopped:", initColl.destBalancerStopped)
}

func (initColl *InitCollection) ResetBalancer() {
	query := bson.M{"_id": "balancer"}
	if initColl.srcIsMongos {
		srcBalancerDocument := bson.M{"stopped": initColl.srcBalancerStopped}
		initColl.srcClient.DB("config").C("settings").Update(query, srcBalancerDocument)
	}
	if initColl.destIsMongos {
		destBalancerDocument := bson.M{"stopped": initColl.destBalancerStopped}
		initColl.destClient.DB("config").C("settings").Update(query, destBalancerDocument)
	}
}

func (initColl *InitCollection) Run() {
	logger.Println("pre checking conn status.")
	initColl.InitConn()
	logger.Println("setting migrate step.")
	initColl.SetStepSign()
	initColl.StopBalancer()
	if DoShardCollection {
		initColl.ShardDestCollection()
		initColl.PreAllocChunks()
	}
}

type CopyData struct {
	initColl   *InitCollection
	workerNum  int
	queryKey   string
	queryChunk []bson.M
}

func NewCopyData(initColl *InitCollection, findAndInsertWorkerNum int) *CopyData {
	return &CopyData{workerNum: findAndInsertWorkerNum, initColl: initColl}
}

// when doing find and insert , we use many goroutine , each goroutine copy one range of data,
// we use the first key of shardkey as the query condition
func (copyData *CopyData) GetQueryKey() {
	if copyData.initColl.srcIsSharded {
		logger.Println("select the first key of shardkey as query condition.")
		copyData.queryKey = copyData.initColl.srcShardKey[0].Name
	} else {
		copyData.queryKey = " "
	}
	logger.Println("query condition key is : ", copyData.queryKey)
}

// one goroutine,find and insert data
func (copyData *CopyData) RangeCopy(chunkQueue chan bson.M, ch chan int) {
	srcMongoUri := GenMongoDBUri(copyData.initColl.src, copyData.initColl.srcUserName, copyData.initColl.srcPassWord)
	destMongoUri := GenMongoDBUri(copyData.initColl.dest, copyData.initColl.destUserName, copyData.initColl.destPassWord)
	srcClient, _ := mgo.Dial(srcMongoUri)
	destClient, _ := mgo.Dial(destMongoUri)
	destClient.EnsureSafe(&mgo.Safe{W: copyData.initColl.writeAck, WMode: copyData.initColl.writeMode, FSync: copyData.initColl.fsync, J: copyData.initColl.journal})
	srcCollConn := srcClient.DB(copyData.initColl.srcDB).C(copyData.initColl.srcColl)
	destCollConn := destClient.DB(copyData.initColl.destDB).C(copyData.initColl.destColl)
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
	if copyData.initColl.srcIsSharded {
		logger.Println("use chunk shardkey range getting query range")
		for _, chunk := range copyData.initColl.srcChunks {
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
	indexes, _ := copyData.initColl.srcCollConn.Indexes()
	var err error
	for _, index := range indexes {
		err = copyData.initColl.destCollConn.EnsureIndex(index)
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
	chs := make([]chan int, len(copyData.initColl.srcOplogNodes))
	i := 0
	for shard, oplogNode := range copyData.initColl.srcOplogNodes {
		chs[i] = make(chan int)
		go copyData.GetLastOpTs(chs[i], shard, oplogNode)
		i++
	}

	for _, ts := range chs {
		<-ts
	}

	for shardAndNode, ts := range oplogSyncTs {
		logger.Println("saved last op ts for ", shardAndNode, " is : ", ts>>32)
	}
}

func (copyData *CopyData) GetLastOpTs(ch chan int, shard string, node string) {
	mongoUri := GenMongoDBUri(node, copyData.initColl.srcUserName, copyData.initColl.srcPassWord)
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
	initColl *InitCollection
}

func NewOplogSync(initColl *InitCollection) *OplogSync {
	return &OplogSync{initColl}
}

func (oplogSync *OplogSync) ApplyOp(oplog bson.M) {
	op := oplog["op"]
	switch op {
	case "i":
		oplogSync.initColl.destCollConn.Insert(oplog["o"])
	case "u":
		oplogSync.initColl.destCollConn.Update(oplog["o2"], oplog["o"])
	case "d":
		oplogSync.initColl.destCollConn.Remove(oplog["o"])
	}
}

func (oplogSync *OplogSync) StartOplogSync(shard string, node string) {
	mongoUri := GenMongoDBUri(node, oplogSync.initColl.srcUserName, oplogSync.initColl.srcPassWord)
	mongoClient, _ := mgo.Dial(mongoUri)
	mongoClient.EnsureSafe(&mgo.Safe{W: oplogSync.initColl.writeAck, WMode: oplogSync.initColl.writeMode, FSync: oplogSync.initColl.fsync, J: oplogSync.initColl.journal})
	var result bson.M

	// ok to use OPLOG_REPLAY here now
	startLastOpTs := oplogSyncTs[shard+"/"+node]
	oplogDB := mongoClient.DB("local").C("oplog.rs")
	oplogQuery := bson.M{"ts": bson.M{"$gte": startLastOpTs}}
	oplogIter := oplogDB.Find(oplogQuery).LogReplay().Sort("$natural").Tail(-1)
	srcNs := oplogSync.initColl.srcDB + "." + oplogSync.initColl.srcColl
	for oplogIter.Next(&result) {
		if ts, ok := result["ts"].(bson.MongoTimestamp); ok {
			oplogSyncTs[shard+"/"+node] = ts
		}

		if result["fromMigrate"] == true {
			continue
		}

		if result["ns"] != srcNs {
			continue
		}

		oplogSync.ApplyOp(result)
	}
}

func (oplogSync *OplogSync) Run() {
	isResetBalancer := false
	for shard, oplogNode := range oplogSync.initColl.srcOplogNodes {
		go oplogSync.StartOplogSync(shard, oplogNode)
	}
	for {
		shouldResetBalancer := true
		for shardAndNode, ts := range oplogSyncTs {
			node := strings.Split(shardAndNode, "/")[1]
			srcMongoUri := GenMongoDBUri(node, oplogSync.initColl.srcUserName, oplogSync.initColl.srcPassWord)
			mongoClient, _ := mgo.Dial(srcMongoUri)
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
			oplogSync.initColl.ResetBalancer()
			isResetBalancer = true
		}
		time.Sleep(1e10)
	}
}

func (oplogSync *OplogSync) LoadProgress() {

}

func (oplogSync *OplogSync) DumpProgress() {

}

func cleanJob() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for sig := range c {
		logger.Println("got ctrl-c,exit ing ...", sig)
		logger.Println("===================================end this job.==================================")
		os.Exit(0)
	}
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

	go cleanJob()

	logger.Println("===================================start one new job.==================================")
	//init step
	logger.Println("start init collection")
	initColl := NewInitCollection(src, dest, srcDB, srcColl, srcUserName, srcPassWord, destDB, destColl, destUserName, destPassWord, writeAck, writeMode, journal, fsync)
	initColl.Run()

	//copy data step

	logger.Println("start copy data")
	copyData := NewCopyData(initColl, findAndInsertWorkerNum)
	copyData.Run()

	//oplog sync step

	logger.Println("start sync oplog")
	oplogSync := NewOplogSync(initColl)
	oplogSync.Run()

}
