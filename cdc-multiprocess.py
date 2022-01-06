from datetime import datetime, timedelta
import os
import sys
import time
import pymongo
from bson.timestamp import Timestamp
import threading
import multiprocessing as mp
import hashlib



def oplog_processor(threadnum, appConfig, perfQ):
    print('processing thread {:>3d} | started'.format(threadnum))

    c = pymongo.MongoClient(appConfig["connectionString"])
    oplog = c.local.oplog.rs

    destConnection = pymongo.MongoClient(appConfig["destConnectionString"])
    destDatabase = destConnection[appConfig["destNs"].split('.',2)[0]]
    destCollection = destDatabase[appConfig["destNs"].split('.',2)[1]]

    '''
    i  = insert
    u  = update
    d  = delete
    c  = command
    db = database
    n  = no-op
    '''

    startTime = time.time()
    lastFeedback = time.time()
    lastBatch = time.time()

    allDone = False
    threadOplogEntries = 0
    threadMaxOplogEntries = appConfig["maxOplogEntries"] // appConfig["numProcessingThreads"]

    bulkOpList = []
    numCurrentBulkOps = 0

    #while not allDone:
    cursor = oplog.find({'ts': {'$gte': appConfig["startTs"]},'ns':appConfig["sourceNs"]},cursor_type=pymongo.CursorType.TAILABLE_AWAIT,oplog_replay=True)
        
    printedFirstTs = False
    myCollectionOps = 0
        
    while cursor.alive and not allDone:
        for doc in cursor:
            endTs = doc['ts']
            
            # NOTE: Python's non-deterministic hash() cannot be used as it is seeded at startup, since this code is multiprocessing we need all hash calls to be the same between processes
            #   hash(str(doc['o']['_id']))
            if (((doc['op'] in ['i','d']) and (doc['ns'] == appConfig["sourceNs"]) and ((int(hashlib.sha512(str(doc['o']['_id']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]) == threadnum)) or
                ((doc['op'] in ['u']) and (doc['ns'] == appConfig["sourceNs"]) and ((int(hashlib.sha512(str(doc['o2']['_id']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]) == threadnum))):
                # this is for my thread

                threadOplogEntries += 1
            
                if (not printedFirstTs) and (doc['op'] in ['i','u','d']) and (doc['ns'] == appConfig["sourceNs"]):
                    print('processing thread {:>3d} | first timestamp = {}'.format(threadnum,doc['ts']))
                    printedFirstTs = True

                if (doc['op'] == 'i'):
                    # insert
                    if (doc['ns'] == appConfig["sourceNs"]):
                        myCollectionOps += 1
                        bulkOpList.append(pymongo.InsertOne(doc['o']))
                        numCurrentBulkOps += 1
                    else:
                        pass
                    
                elif (doc['op'] == 'u'):
                    # update
                    if (doc['ns'] == appConfig["sourceNs"]):
                        myCollectionOps += 1
                        doc['o'].pop('$v')
                        bulkOpList.append(pymongo.UpdateOne(doc['o2'],doc['o'],upsert=False))
                        numCurrentBulkOps += 1
                    else:
                        pass
                        
                elif (doc['op'] == 'd'):
                    # delete
                    if (doc['ns'] == appConfig["sourceNs"]):
                        myCollectionOps += 1
                        bulkOpList.append(pymongo.DeleteOne(doc['o']))
                        numCurrentBulkOps += 1
                    else:
                        pass
                        
                elif (doc['op'] == 'c'):
                    # command
                    pass
                        
                elif (doc['op'] == 'n'):
                    # no-op
                    pass
                        
                else:
                    print(doc)
                    sys.exit(1)
                    
                if ((numCurrentBulkOps >= appConfig["numBulkOperationsPerCall"]) or ((lastBatch + appConfig["maxSecondsBetweenBatches"]) < time.time())) and (numCurrentBulkOps > 0):
                    result = destCollection.bulk_write(bulkOpList,ordered=True)
                    perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps})
                    bulkOpList = []
                    numCurrentBulkOps = 0
                    lastBatch = time.time()
                
                if (threadOplogEntries > threadMaxOplogEntries):
                    allDone = True
                    break
                    
            if ((numCurrentBulkOps >= appConfig["numBulkOperationsPerCall"]) or ((lastBatch + appConfig["maxSecondsBetweenBatches"]) < time.time())) and (numCurrentBulkOps > 0):
                result = destCollection.bulk_write(bulkOpList,ordered=True)
                perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps})
                bulkOpList = []
                numCurrentBulkOps = 0
                lastBatch = time.time()
        
    if (numCurrentBulkOps > 0):
        result = destCollection.bulk_write(bulkOpList,ordered=True)
        perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps})
        bulkOpList = []
        numCurrentBulkOps = 0
            
    c.close()
    
    perfQ.put({"name":"processCompleted","processNum":threadNum})
    

def reporter(appConfig, perfQ):
    print('reporting thread | started')
    
    startTime = time.time()
    lastTime = time.time()
    
    lastProcessedOplogEntries = 0
    nextReportTime = startTime + appConfig["numSecondsFeedback"]
    
    numWorkersCompleted = 0
    numProcessedOplogEntries = 0
    
    while (numWorkersCompleted < appConfig["numProcessingThreads"]):
        time.sleep(appConfig["numSecondsFeedback"])
        nowTime = time.time()
        
        while not perfQ.empty():
            qMessage = perfQ.get_nowait()
            if qMessage['name'] == "batchCompleted":
                numProcessedOplogEntries += qMessage['operations']
            elif qMessage['name'] == "processCompleted":
                numWorkersCompleted += 1

        # total total
        elapsedSeconds = nowTime - startTime
        totalOpsPerSecond = numProcessedOplogEntries / elapsedSeconds

        # elapsed hours, minutes, seconds
        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)
        
        # this interval
        intervalElapsedSeconds = nowTime - lastTime
        intervalOpsPerSecond = (numProcessedOplogEntries - lastProcessedOplogEntries) / intervalElapsedSeconds
        
        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        print("[{0}] elapsed {1} | total ops {2:12,.2f} | interval ops {3:12,.2f} | tot {4:16,d}".format(logTimeStamp,thisHMS,totalOpsPerSecond,intervalOpsPerSecond,numProcessedOplogEntries))
        nextReportTime = nowTime + appConfig["numSecondsFeedback"]
        
        lastTime = nowTime
        lastProcessedOplogEntries = numProcessedOplogEntries


def main():
    appConfig = {
        # start from the beginning of the oplog rather than an aribtrary timestamp
        "startFromOplogStart" : True,
        # source and destination collections
        "sourceNs" : 'cdctest7.coll',
        "destNs" : 'cdctest7.coll',
        # number of seconds between console performance output info
        "numSecondsFeedback" : 2,
        # stop code after processing specific number of oplog entries, each thread takes it's portion of these
        "maxOplogEntries" : 2000000000,
        # batch size for insert/update/delete, batches can be smaller if numSecondsBetweenBatches is exceeded
        "numBulkOperationsPerCall" : 100,
        # number of seconds between batches, in case less than the number of batch entries has beeen found
        "maxSecondsBetweenBatches" : 5,
        # number of thread to process oplog in parallel
        "numProcessingThreads" : 8
    }

    print("processing oplog across {0} threads".format(appConfig["numProcessingThreads"]))

    # source server setup
    dbHost = os.environ.get('DOCDB_HOST')
    dbUsername = os.environ.get('DOCDB_USERNAME')
    dbPassword = os.environ.get('DOCDB_PASSWORD')

    if ".docdb." in dbHost:
        appConfig["connectionString"] = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&retryWrites=false'
        print('connecting to DocumentDB at {}'.format(dbHost))
    else:
        appConfig["connectionString"] = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?replicaSet=rs0&retryWrites=false'
        print('connecting to MongoDB at {}'.format(dbHost))

    c = pymongo.MongoClient(appConfig["connectionString"])
    oplog = c.local.oplog.rs
    appConfig["startTs"] = Timestamp(0, 1)

    if appConfig["startFromOplogStart"]:
        # start with first oplog entry
        first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(1).next()
        appConfig["startTs"] = first['ts']
    else:
        # start at an arbitrary position
        #appConfig["startTs"] = Timestamp(1641240727, 5)
        # start with right now
        appConfig["startTs"] = Timestamp(int(time.time()), 1)
        
    print("starting with timestamp = {}".format(appConfig["startTs"]))

    c.close()
    
    # destination server setup
    dbDestHost = os.environ.get('DEST_HOST')
    dbDestUsername = os.environ.get('DEST_USERNAME')
    dbDestPassword = os.environ.get('DEST_PASSWORD')

    if ".docdb." in dbDestHost:
        #connectionString = 'mongodb://'+dbDestUsername+':'+dbDestPassword+'@'+dbDestHost+'/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&retryWrites=false'
        appConfig["destConnectionString"] = 'mongodb://'+dbDestUsername+':'+dbDestPassword+'@'+dbDestHost+'/?ssl=false&replicaSet=rs0&retryWrites=false'
        print('connecting to Destination DocumentDB at {}'.format(dbDestHost))
    else:
        appConfig["destConnectionString"] = 'mongodb://'+dbDestUsername+':'+dbDestPassword+'@'+dbDestHost+'/?replicaSet=rs0&retryWrites=false'
        print('connecting to Destination MongoDB at {}'.format(dbDestHost))

    mp.set_start_method('spawn')
    q = mp.Queue()

    t = threading.Thread(target=reporter,args=(appConfig,q))
    t.start()
    
    processList = []
    for loop in range(appConfig["numProcessingThreads"]):
        p = mp.Process(target=oplog_processor,args=(loop,appConfig,q))
        processList.append(p)
        
    for process in processList:
        process.start()
        
    for process in processList:
        process.join()
        
    t.join()


if __name__ == "__main__":
    main()
