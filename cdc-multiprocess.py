from datetime import datetime, timedelta
import os
import sys
import time
import pymongo
from bson.timestamp import Timestamp
import threading
import multiprocessing as mp
import hashlib
import argparse


def logIt(threadnum, message):
    logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
    print("[{}] thread {:>3d} | {}".format(logTimeStamp,threadnum,message))


def oplog_processor(threadnum, appConfig, perfQ):
    if appConfig['verboseLogging']:
        logIt(threadnum,'thread started')

    c = pymongo.MongoClient(appConfig["sourceUri"])
    oplog = c.local.oplog.rs

    destConnection = pymongo.MongoClient(appConfig["targetUri"])
    destDatabase = destConnection[appConfig["targetNs"].split('.',2)[0]]
    destCollection = destDatabase[appConfig["targetNs"].split('.',2)[1]]

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

    bulkOpList = []
    
    # list with replace, not insert, in case document already exists (replaying old oplog)
    bulkOpListReplace = []
    numCurrentBulkOps = 0
    
    numTotalBatches = 0
        
    printedFirstTs = False
    myCollectionOps = 0

    # starting timestamp
    endTs = appConfig["startTs"]

    while not allDone:
        if appConfig['verboseLogging']:
            logIt(threadnum,"Creating oplog tailing cursor for timestamp {}".format(endTs.as_datetime()))

        cursor = oplog.find({'ts': {'$gte': endTs},'ns':appConfig["sourceNs"]},cursor_type=pymongo.CursorType.TAILABLE_AWAIT,oplog_replay=True)

        while cursor.alive and not allDone:
            for doc in cursor:
                # check if time to exit
                if ((time.time() - startTime) > appConfig['durationSeconds']) and (appConfig['durationSeconds'] != 0):
                    allDone = True
                    break

                endTs = doc['ts']

                # NOTE: Python's non-deterministic hash() cannot be used as it is seeded at startup, since this code is multiprocessing we need all hash calls to be the same between processes
                #   hash(str(doc['o']['_id']))
                if (((doc['op'] in ['i','d']) and (doc['ns'] == appConfig["sourceNs"]) and ((int(hashlib.sha512(str(doc['o']['_id']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]) == threadnum)) or
                    ((doc['op'] in ['u']) and (doc['ns'] == appConfig["sourceNs"]) and ((int(hashlib.sha512(str(doc['o2']['_id']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]) == threadnum))):
                    # this is for my thread

                    threadOplogEntries += 1

                    if (not printedFirstTs) and (doc['op'] in ['i','u','d']) and (doc['ns'] == appConfig["sourceNs"]):
                        if appConfig['verboseLogging']:
                            logIt(threadnum,'first timestamp = {} aka {}'.format(doc['ts'],doc['ts'].as_datetime()))
                        printedFirstTs = True

                    if (doc['op'] == 'i'):
                        # insert
                        if (doc['ns'] == appConfig["sourceNs"]):
                            myCollectionOps += 1
                            bulkOpList.append(pymongo.InsertOne(doc['o']))
                            # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                            bulkOpListReplace.append(pymongo.ReplaceOne({'_id':doc['o']['_id']},doc['o'],upsert=True))
                            numCurrentBulkOps += 1
                        else:
                            pass

                    elif (doc['op'] == 'u'):
                        # update
                        if (doc['ns'] == appConfig["sourceNs"]):
                            myCollectionOps += 1
                            # field "$v" is not present in MongoDB 3.4
                            doc['o'].pop('$v',None)
                            bulkOpList.append(pymongo.UpdateOne(doc['o2'],doc['o'],upsert=False))
                            # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                            bulkOpListReplace.append(pymongo.UpdateOne(doc['o2'],doc['o'],upsert=False))
                            numCurrentBulkOps += 1
                        else:
                            pass

                    elif (doc['op'] == 'd'):
                        # delete
                        if (doc['ns'] == appConfig["sourceNs"]):
                            myCollectionOps += 1
                            bulkOpList.append(pymongo.DeleteOne(doc['o']))
                            # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                            bulkOpListReplace.append(pymongo.DeleteOne(doc['o']))
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

                if ((numCurrentBulkOps >= appConfig["maxOperationsPerBatch"]) or (time.time() >= (lastBatch + appConfig["maxSecondsBetweenBatches"]))) and (numCurrentBulkOps > 0):
                    if not appConfig['dryRun']:
                        try:
                            result = destCollection.bulk_write(bulkOpList,ordered=True)
                        except:
                            # replace inserts as replaces
                            result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
                    perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"endts":endTs})
                    bulkOpList = []
                    bulkOpListReplace = []
                    numCurrentBulkOps = 0
                    numTotalBatches += 1
                    lastBatch = time.time()

            if ((numCurrentBulkOps >= appConfig["maxOperationsPerBatch"]) or (time.time() >= (lastBatch + appConfig["maxSecondsBetweenBatches"]))) and (numCurrentBulkOps > 0):
                if not appConfig['dryRun']:
                    try:
                        result = destCollection.bulk_write(bulkOpList,ordered=True)
                    except:
                        # replace inserts as replaces
                        result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
                perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"endts":endTs})
                bulkOpList = []
                bulkOpListReplace = []
                numCurrentBulkOps = 0
                numTotalBatches += 1
                lastBatch = time.time()

            # nothing arrived in the oplog for 1 second, pause before trying again
            time.sleep(1)

    if (numCurrentBulkOps > 0):
        if not appConfig['dryRun']:
            try:
                result = destCollection.bulk_write(bulkOpList,ordered=True)
            except:
                # replace inserts as replaces
                result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
        perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"endts":endTs})
        bulkOpList = []
        bulkOpListReplace = []
        numCurrentBulkOps = 0
        numTotalBatches += 1

    c.close()

    perfQ.put({"name":"processCompleted","processNum":threadnum})


def reporter(appConfig, perfQ):
    if appConfig['verboseLogging']:
        logIt(-1,'reporting thread started')
    
    startTime = time.time()
    lastTime = time.time()
    
    lastProcessedOplogEntries = 0
    nextReportTime = startTime + appConfig["feedbackSeconds"]
    
    numWorkersCompleted = 0
    numProcessedOplogEntries = 0
    
    oldestDt = datetime(2099,12,31,1,1,1)
    newestDt = datetime(2000,1,1,1,1,1)
    
    while (numWorkersCompleted < appConfig["numProcessingThreads"]):
        time.sleep(appConfig["feedbackSeconds"])
        nowTime = time.time()
        
        while not perfQ.empty():
            qMessage = perfQ.get_nowait()
            if qMessage['name'] == "batchCompleted":
                numProcessedOplogEntries += qMessage['operations']
                thisEndDt = qMessage['endts'].as_datetime().replace(tzinfo=None)
                if (thisEndDt > newestDt):
                    newestDt = thisEndDt
                if (thisEndDt < oldestDt):
                    oldestDt = thisEndDt
                #print("received endTs = {}".format(thisEndTs.as_datetime()))
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
        
        # how far behind current time
        oldTdBehind = datetime.utcnow() - oldestDt.replace(tzinfo=None)
        oldSecondsBehind = int(oldTdBehind.total_seconds())
        newTdBehind = datetime.utcnow() - newestDt.replace(tzinfo=None)
        newSecondsBehind = int(newTdBehind.total_seconds())
        
        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        print("[{0}] elapsed {1} | total o/s {2:12,.2f} | interval o/s {3:12,.2f} | tot {4:16,d} | oldest {5} | {6:12,d} secs | newest {7} | {8:12,d} secs".format(logTimeStamp,thisHMS,totalOpsPerSecond,intervalOpsPerSecond,numProcessedOplogEntries,oldestDt,oldSecondsBehind,newestDt,newSecondsBehind))
        nextReportTime = nowTime + appConfig["feedbackSeconds"]
        
        lastTime = nowTime
        lastProcessedOplogEntries = numProcessedOplogEntries


def main():
    parser = argparse.ArgumentParser(description='CDC replication tool.')

    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')
    
    parser.add_argument('--source-uri',
                        required=True,
                        type=str,
                        help='Source URI')

    parser.add_argument('--target-uri',
                        required=True,
                        type=str,
                        help='Target URI')

    parser.add_argument('--source-namespace',
                        required=True,
                        type=str,
                        help='Source Namespace as <database>.<collection>')
                        
    parser.add_argument('--target-namespace',
                        required=False,
                        type=str,
                        help='Target Namespace as <database>.<collection>, defaults to --source-namespace')
                        
    parser.add_argument('--duration-seconds',
                        required=False,
                        type=int,
                        default=0,
                        help='Number of seconds to run before existing, 0 = run forever')

    parser.add_argument('--feedback-seconds',
                        required=False,
                        type=int,
                        default=60,
                        help='Number of seconds between feedback output')

    parser.add_argument('--threads',
                        required=False,
                        type=int,
                        default=1,
                        help='Number of threads (parallel processing)')

    parser.add_argument('--max-seconds-between-batches',
                        required=False,
                        type=int,
                        default=5,
                        help='Maximum number of seconds to await full batch')

    parser.add_argument('--max-operations-per-batch',
                        required=False,
                        type=int,
                        default=100,
                        help='Maximum number of operations to include in a single batch')
                        
    parser.add_argument('--dry-run',
                        required=False,
                        action='store_true',
                        help='Read source changes only, do not apply to target')

    parser.add_argument('--start-position',
                        required=True,
                        type=str,
                        help='Starting position - 0 for all available changes, otherwise YYYY-MM-DD+HH:MM:SS in UTC')

    parser.add_argument('--verbose',
                        required=False,
                        action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['sourceUri'] = args.source_uri
    appConfig['targetUri'] = args.target_uri
    appConfig['numProcessingThreads'] = args.threads
    appConfig['maxSecondsBetweenBatches'] = args.max_seconds_between_batches
    appConfig['maxOperationsPerBatch'] = args.max_operations_per_batch
    appConfig['durationSeconds'] = args.duration_seconds
    appConfig['feedbackSeconds'] = args.feedback_seconds
    appConfig['dryRun'] = args.dry_run
    appConfig['sourceNs'] = args.source_namespace
    if not args.target_namespace:
        appConfig['targetNs'] = args.source_namespace
    else:
        appConfig['targetNs'] = args.target_namespace
    appConfig['startPosition'] = args.start_position
    appConfig['verboseLogging'] = args.verbose
    
    logIt(-1,"processing oplog using {0} threads".format(appConfig['numProcessingThreads']))

    c = pymongo.MongoClient(appConfig["sourceUri"])
    oplog = c.local.oplog.rs
    appConfig["startTs"] = Timestamp(0, 1)

    if appConfig["startPosition"] == "0":
        # start with first oplog entry
        first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(1).next()
        appConfig["startTs"] = first['ts']
    else:
        # start at an arbitrary position
        #appConfig["startTs"] = Timestamp(1641240727, 5)
        # start with right now
        #appConfig["startTs"] = Timestamp(int(time.time()), 1)
        appConfig["startTs"] = Timestamp(datetime.fromisoformat(args.start_position), 1)
        
    logIt(-1,"starting with timestamp = {}".format(appConfig["startTs"].as_datetime()))

    c.close()
    
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
