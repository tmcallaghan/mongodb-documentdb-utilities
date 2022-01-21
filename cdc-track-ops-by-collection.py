import os
import sys
import time
import pymongo
from bson.timestamp import Timestamp
import threading

# start from the beginning of the oplog rather than an aribtrary timestamp
startFromOplogStart = True

# consume all of the oplog rather than scoping to a single namespace
consumeEntireOplog = True

numForFeedback = 100000
numSecondsForFeedback = 5
maxOplogEntries = 6100000
numBulkOperationsPerCall = 100
maxSecondsBetweenBatches = 1

mongoWireCompression="none"
#mongoWireCompression="zlib"
#mongoWireCompression="snappy"

# source server
dbHost = os.environ.get('DOCDB_HOST')
dbUsername = os.environ.get('DOCDB_USERNAME')
dbPassword = os.environ.get('DOCDB_PASSWORD')

if ".docdb." in dbHost:
    connectionString = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false'
    print('connecting to DocumentDB at {}'.format(dbHost))
else:
    extraUri = ""
    if mongoWireCompression != "none":
        extraUri = "&compressors={}".format(mongoWireCompression)
        print("...enabling MongoDB wire compression using {}".format(mongoWireCompression))
    else:
        print("...not compressing MongoDB wire traffic")
    connectionString = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false'+extraUri
    print('connecting to MongoDB at {}'.format(dbHost))

c = pymongo.MongoClient(connectionString)


oplog = c.local.oplog.rs
startTs = Timestamp(0, 1)

if startFromOplogStart:
    # start with first oplog entry
    first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(1).next()
    startTs = first['ts']
    print(first)
else:
    # start at an arbitrary position
    startTs = Timestamp(1641240727, 5)
    # start with right now
    #startTs = Timestamp(int(time.time()), 1)
    
    

print("starting with timestamp = {}".format(startTs))

numTotalOplogEntries = 0
numProcessedOplogEntries = 0
opDict = {}

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
allDone = False

sourceNs = "<database>.<collection>"

while not allDone:
    if consumeEntireOplog:
        cursor = oplog.find({'ts': {'$gte': startTs}},cursor_type=pymongo.CursorType.TAILABLE_AWAIT,oplog_replay=True)
    
    else:
        cursor = oplog.find({'ts': {'$gte': startTs},'ns':sourceNs},cursor_type=pymongo.CursorType.TAILABLE_AWAIT,oplog_replay=True)
        
    printedFirstTs = False
    myCollectionOps = 0
        
    while cursor.alive and not allDone:
        for doc in cursor:
            endTs = doc['ts']
            
            numTotalOplogEntries += 1
            if ((numTotalOplogEntries % numForFeedback) == 0) or ((lastFeedback + numSecondsForFeedback) < time.time()):
                lastFeedback = time.time()
                elapsedSeconds = time.time() - startTime
                if (elapsedSeconds != 0):
                    print("  tot read {:16,d} / {:12,.0f} ops | coll read {:16,d} / {:12,.0f} ops | processed {:16,d} / {:12,.0f} ops".format(numTotalOplogEntries,numTotalOplogEntries//elapsedSeconds,myCollectionOps,myCollectionOps//elapsedSeconds,numProcessedOplogEntries,numProcessedOplogEntries//elapsedSeconds))
                else:
                    print("  tot read {:16,d} oplog entries in {:12,.0f} ops".format(numTotalOplogEntries,0.00))

            #if (not printedFirstTs) and (doc['op'] in ['i','u','d']) and (doc['ns'] == sourceNs):
            #    print("*** first timestamp = {}".format(doc['ts']))
            #    printedFirstTs = True

            if (doc['op'] == 'i'):
                # insert
                thisOp = doc['ns']+'|insert'
                if thisOp in opDict:
                    opDict[thisOp] += 1
                else:
                    opDict[thisOp] = 1
                    
            elif (doc['op'] == 'u'):
                # update
                thisOp = doc['ns']+'|update'
                if thisOp in opDict:
                    opDict[thisOp] += 1
                else:
                    opDict[thisOp] = 1
                    
            elif (doc['op'] == 'd'):
                # delete
                thisOp = doc['ns']+'|delete'
                if thisOp in opDict:
                    opDict[thisOp] += 1
                else:
                    opDict[thisOp] = 1
                    
            elif (doc['op'] == 'c'):
                # command
                thisOp = '**command**|'+doc['ns']
                if thisOp in opDict:
                    opDict[thisOp] += 1
                else:
                    opDict[thisOp] = 1
                    
            elif (doc['op'] == 'n'):
                # no-op
                thisOp = '**NO-OP**'
                if thisOp in opDict:
                    opDict[thisOp] += 1
                else:
                    opDict[thisOp] = 1
                    
            else:
                print(doc)
                sys.exit(1)
                
            if numTotalOplogEntries > maxOplogEntries:
                allDone = True
                break
                

# print overall ops, ips/ups/dps

print("")
print("-----------------------------------------------------------------------------------------")
print("")
oplogSeconds = (endTs.as_datetime()-startTs.as_datetime()).total_seconds()
print("opLog elapsed seconds = {}".format(oplogSeconds))

# print collection ops, ips/ups/dps
for thisOpKey in sorted(opDict.keys()):
    print("collection|op = {:60} : number of operations = {:16,d} @ {} ops/sec".format(thisOpKey,opDict[thisOpKey],opDict[thisOpKey]//oplogSeconds))

print("")
        
c.close()
