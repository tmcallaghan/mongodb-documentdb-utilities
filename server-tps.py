from datetime import datetime, timedelta
import sys
import json
import pymongo
from pymongo import InsertOne, DeleteOne, ReplaceOne, UpdateOne
import time
import os


numSecondsFeedback = 5
numIntervalsTps = 5

dbHost = os.environ.get('DOCDB_HOST')
dbUsername = os.environ.get('DOCDB_USERNAME')
dbPassword = os.environ.get('DOCDB_PASSWORD')

if ".docdb." in dbHost:
    #connectionString = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&retryWrites=false'
    connectionString = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?ssl=false&retryWrites=false'
    print('connecting to DocumentDB at {}'.format(dbHost))
else:
    connectionString = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false'
    print('connecting to MongoDB at {}'.format(dbHost))

'''
Everything Available

rs0 [direct: primary] test> db.serverStatus()
{
  host: 'socialite-2xl-graviton3.cy5ood2blnmn.us-east-1.docdb.amazonaws.com:27017',
  version: '4.0.0',
  localTime: ISODate("2021-12-29T18:31:54.000Z"),
  uptime: 605674,
  uptimeMillis: Long("605674197"),
  connections: {
    current: Long("263"),
    available: Long("6837"),
    totalCreated: Long("1634")
  },
  opcounters: {
    insert: Long("16553979"),
    query: Long("92955"),
    update: Long("0"),
    delete: Long("434"),
    getmore: Long("22433"),
    command: Long("844824")
  },
  metrics: {
    commands: { '<UNKNOWN>': Long("9") },
    cursor: {
      timedOut: Long("8"),
      open: { noTimeout: Long("0"), pinned: Long("0"), total: Long("0") }
    },
    document: {
      deleted: Long("0"),
      inserted: Long("16553979"),
      returned: Long("5149776"),
      updated: Long("0")
    },
    ttl: { deletedDocuments: Long("0"), passes: Long("10094") }
  },
  transactions: {
    currentActive: Long("0"),
    currentInactive: Long("0"),
    currentOpen: Long("0"),
    totalAborted: Long("0"),
    totalCommitted: Long("0"),
    totalStarted: Long("0")
  },
  ok: 1,
  operationTime: Timestamp({ t: 1640802714, i: 1 })
}
'''

def reporter():
    startDocIns = 0
    startDocUpd = 0
    startDocDel = 0
    startDocRet = 0
    startOpIns = 0
    startOpUpd = 0
    startOpDel = 0
    startOpQry = 0
    
    currDocIns = 0
    currDocUpd = 0
    currDocDel = 0
    currDocRet = 0
    currOpIns = 0
    currOpUpd = 0
    currOpDel = 0
    currOpQry = 0
    
    lastDocIns = 0
    lastDocUpd = 0
    lastDocDel = 0
    lastDocRet = 0
    lastOpIns = 0
    lastOpUpd = 0
    lastOpDel = 0
    lastOpQry = 0

    recentTps = []
    
    startTime = time.time()
    lastTime = time.time()
    
    nextReportTime = startTime + numSecondsFeedback
    
    client = pymongo.MongoClient(connectionString)

    #cmdDict = client.admin.command("serverStatus")["metrics"]["document"]
    cmdDict = client.admin.command("serverStatus")
    startDocIns = cmdDict["metrics"]["document"]["inserted"]
    startDocUpd = cmdDict["metrics"]["document"]["updated"]
    startDocDel = cmdDict["metrics"]["document"]["deleted"]
    startDocRet = cmdDict["metrics"]["document"]["returned"]
    startOpIns = cmdDict["opcounters"]["insert"]
    startOpUpd = cmdDict["opcounters"]["update"]
    startOpDel = cmdDict["opcounters"]["delete"]
    startOpQry = cmdDict["opcounters"]["query"]
    lastDocIns = startDocIns
    lastDocUpd = startDocUpd
    lastDocDel = startDocDel
    lastDocRet = startDocRet
    lastOpIns = startOpIns
    lastOpUpd = startOpUpd
    lastOpDel = startOpDel
    lastOpQry = startOpQry
    
    while True:
        time.sleep(numSecondsFeedback)
        
        cmdDict = client.admin.command("serverStatus")
        currDocIns = cmdDict["metrics"]["document"]["inserted"]
        currDocUpd = cmdDict["metrics"]["document"]["updated"]
        currDocDel = cmdDict["metrics"]["document"]["deleted"]
        currDocRet = cmdDict["metrics"]["document"]["returned"]
        currOpIns = cmdDict["opcounters"]["insert"]
        currOpUpd = cmdDict["opcounters"]["update"]
        currOpDel = cmdDict["opcounters"]["delete"]
        currOpQry = cmdDict["opcounters"]["query"]

        nowTime = time.time()

        # totals
        elapsedSeconds = nowTime - startTime
        totDocIps = (currDocIns - startDocIns) / elapsedSeconds
        totDocUps = (currDocUpd - startDocUpd) / elapsedSeconds
        totDocDps = (currDocDel - startDocDel) / elapsedSeconds
        totDocRps = (currDocRet - startDocRet) / elapsedSeconds
        totOpIps = (currOpIns - startOpIns) / elapsedSeconds
        totOpUps = (currOpUpd - startOpUpd) / elapsedSeconds
        totOpDps = (currOpDel - startOpDel) / elapsedSeconds
        totOpQps = (currOpQry - startOpQry) / elapsedSeconds
        
        # interval
        intDocIps = (currDocIns - lastDocIns) / numSecondsFeedback
        intDocUps = (currDocUpd - lastDocUpd) / numSecondsFeedback
        intDocDps = (currDocDel - lastDocDel) / numSecondsFeedback
        intDocRps = (currDocRet - lastDocRet) / numSecondsFeedback
        intOpIps = (currOpIns - lastOpIns) / numSecondsFeedback
        intOpUps = (currOpUpd - lastOpUpd) / numSecondsFeedback
        intOpDps = (currOpDel - lastOpDel) / numSecondsFeedback
        intOpQps = (currOpQry - lastOpQry) / numSecondsFeedback

        # elapsed hours, minutes, seconds
        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)

        '''
        # recent intervals
        if len(recentTps) == numIntervalsTps:
            recentTps.pop(0)
        recentTps.append(intervalInsertsPerSecond)
        totRecentTps = 0
        for thisTps in recentTps:
            totRecentTps += thisTps
        avgRecentTps = totRecentTps / len(recentTps)
        '''
        
        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        #print("[{0}] elapsed {7} | total ins/upd {1:16,d} at {2:12,.2f} p/s | tot docs {6:16,d} | last {5} {3:12,.2f} p/s | interval {4:12,.2f} p/s | ins/upd {8:16,d}/{9:16,d} | lat (ms) {10:12} | {11} MB/s".format(logTimeStamp,totalInserts,insertsPerSecond,avgRecentTps,intervalInsertsPerSecond,numIntervalsTps,totalInserts+numExistingDocuments,thisHMS,numTotalInserts,numTotalUpdates,intervalLatencyMs,intervalMBps))
        print("[{0}] elapsed {1} | doc tot ips/ups/dps/rps/all {2:10,.1f}{3:10,.1f}{4:10,.1f}{5:10,.1f}{6:10,.1f} | doc int ips/ups/dps/rps/all {7:10,.1f}{8:10,.1f}{9:10,.1f}{10:10,.1f}{11:10,.1f}".format(
          logTimeStamp,thisHMS,totDocIps,totDocUps,totDocDps,totDocRps,totDocIps+totDocUps+totDocDps+totDocRps,intDocIps,intDocUps,intDocDps,intDocRps,intDocIps+intDocUps+intDocDps+intDocRps))

        print("                                               | ops tot ips/ups/dps/qps/all {2:10,.1f}{3:10,.1f}{4:10,.1f}{5:10,.1f}{6:10,.1f} | ops int ips/ups/dps/qps/all {7:10,.1f}{8:10,.1f}{9:10,.1f}{10:10,.1f}{11:10,.1f}".format(
          logTimeStamp,thisHMS,totOpIps,totOpUps,totOpDps,totOpQps,totOpIps+totOpUps+totOpDps+totOpQps,intOpIps,intOpUps,intOpDps,intOpQps,intOpIps+intOpUps+intOpDps+intOpQps))
          
        nextReportTime = nowTime + numSecondsFeedback
        
        lastTime = nowTime
        
        lastDocIns = currDocIns
        lastDocUpd = currDocUpd
        lastDocDel = currDocDel
        lastDocRet = currDocRet
        lastOpIns = currOpIns
        lastOpUpd = currOpUpd
        lastOpDel = currOpDel
        lastOpQry = currOpQry
            
    '''
    # final report
    elapsedSeconds = time.time() - startTime
    totalInserts = numBatchesCompleted * numInsertsPerBatch
    insertsPerSecond = totalInserts / elapsedSeconds
    logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
    # elapsed hours, minutes, seconds
    thisHours, rem = divmod(elapsedSeconds, 3600)
    thisMinutes, thisSeconds = divmod(rem, 60)
    thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)
    print("[{0}] elapsed {7} | total ins/upd {1:16,d} at {2:12,.2f} p/s | tot docs {6:16,d} | last {5} {3:12,.2f} p/s | interval {4:12,.2f} p/s | ins/upd {8:16,d}/{9:16,d} | lat (ms) {10:12}".format(logTimeStamp,totalInserts,insertsPerSecond,0.0,0.0,numIntervalsTps,totalInserts+numExistingDocuments,thisHMS,numTotalInserts,numTotalUpdates,intervalLatencyMs))
    '''


def main():
    reporter()


if __name__ == "__main__":
    main()
