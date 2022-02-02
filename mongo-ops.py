from datetime import datetime, timedelta
import sys
import json
import pymongo
import time
import os


def reporter(appConfig):
    startServerOpCounters = {}
    startServerMetricsDocument = {}
    startCollectionStats = {}
    startServerUptime = 0
    startServerHost = ''
    startServerLocalTime = ''

    lastServerOpCounters = {}
    lastServerMetricsDocument = {}
    lastCollectionStats = {}

    currentServerOpCounters = {}
    currentServerMetricsDocument = {}
    currentCollectionStats = {}
    currentServerUptime = 0
    currentServerHost = ''
    currentServerLocalTime = ''
    
    startTime = time.time()
    
    print('connecting to MongoDB at {}'.format(appConfig['dbHost']))
    client = pymongo.MongoClient(appConfig['connectionString'])

    startServerOpCounters = client.admin.command("serverStatus")['opcounters']
    startServerMetricsDocument = client.admin.command("serverStatus")['metrics']['document']
    startServerUptime = client.admin.command("serverStatus")['uptime']
    startServerHost = client.admin.command("serverStatus")['host']
    startServerLocalTime = client.admin.command("serverStatus")['localTime']
    startCollectionStats = getCollectionStats(client)
    
    lastServerOpCounters = startServerOpCounters.copy()
    lastServerMetricsDocument = startServerMetricsDocument.copy()
    lastCollectionStats = startCollectionStats.copy()
    
    elapsedSeconds = 0
    
    while (elapsedSeconds < appConfig['numRunSeconds']):
        time.sleep(appConfig['numSecondsFeedback'])
        
        currentServerOpCounters = client.admin.command("serverStatus")['opcounters']
        currentServerMetricsDocument = client.admin.command("serverStatus")['metrics']['document']
        currentServerUptime = client.admin.command("serverStatus")['uptime']
        currentServerHost = client.admin.command("serverStatus")['host']
        currentServerLocalTime = client.admin.command("serverStatus")['localTime']
        currentCollectionStats = getCollectionStats(client)

        nowTime = time.time()
        elapsedSeconds = nowTime - startTime
        # elapsed hours, minutes, seconds
        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)
        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        print("------------------------------------------------------------------------------------------------------")
        print("{} | {} | checking for changes".format(logTimeStamp,thisHMS))
        
        printDiffs('serverOpCounters',currentServerOpCounters,lastServerOpCounters)
        printDiffs('serverDocumentMetrics',currentServerMetricsDocument,lastServerMetricsDocument)
        for thisDb in currentCollectionStats:
            for thisColl in currentCollectionStats[thisDb]:
                printDiffs("{}.{}".format(thisDb,thisColl),currentCollectionStats[thisDb][thisColl],lastCollectionStats[thisDb][thisColl])
        
        lastServerOpCounters = currentServerOpCounters.copy()
        lastServerMetricsDocument = currentServerMetricsDocument.copy()
        lastCollectionStats = currentCollectionStats.copy()
        
    # log what we found
    finalDict = {}
    finalDict['serverAlias'] = appConfig['serverAlias']
    finalDict['start'] = {}
    finalDict['start']['opcounters'] = startServerOpCounters
    finalDict['start']['docmetrics'] = startServerMetricsDocument
    finalDict['start']['uptime'] = startServerUptime
    finalDict['start']['host'] = startServerHost
    finalDict['start']['localtime'] = startServerLocalTime
    finalDict['start']['collstats'] = startCollectionStats
    finalDict['end'] = {}
    finalDict['end']['opcounters'] = currentServerOpCounters
    finalDict['end']['docmetrics'] = currentServerMetricsDocument
    finalDict['end']['uptime'] = currentServerUptime
    finalDict['end']['host'] = currentServerHost
    finalDict['end']['localtime'] = currentServerLocalTime
    finalDict['end']['collstats'] = currentCollectionStats
    #logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
    logTimeStamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    logFileName = "{}-{}-mongo-ops.json".format(appConfig['serverAlias'],logTimeStamp)
    with open(logFileName, 'w') as fp:
        json.dump(finalDict, fp, indent=4, default=str)
        
    client.close()


def getCollectionStats(client):
    returnDict = {}
    
    # get databases - filter out admin, config, local, and system
    dbDict = client.admin.command("listDatabases",nameOnly=True,filter={"name":{"$nin":['admin','config','local','system']}})['databases']
    for thisDb in dbDict:
        collCursor = client[thisDb['name']].list_collections()
        for thisColl in collCursor:
            if thisColl['type'] == 'view':
                #print("  skipping view {}".format(thisColl['name']))
                pass
            else:
                collStats = client[thisDb['name']].command("collstats",thisColl['name'])['wiredTiger']['cursor']
                if thisDb['name'] not in returnDict:
                    returnDict[thisDb['name']] = {}
                returnDict[thisDb['name']][thisColl['name']] = collStats.copy()
    
    return returnDict
    
    
def printDiffs(diffType,dict1,dict2):
    for key in dict1:
        if (dict1[key] != dict2[key]):
            print('  {} | {} | {}'.format(diffType,key,dict1[key]-dict2[key]))
    

def main():
    if len(sys.argv) != 3:
        print("")
        print("usage: python3 mongo-ops.py <number-of-seconds-to-run-for> <nickname-for-server>")
        print("")
        
    else:
        appConfig = {}
        appConfig['numRunSeconds'] = int(sys.argv[1])
        appConfig['serverAlias'] = sys.argv[2]
        appConfig['numSecondsFeedback'] = 60
        appConfig['dbHost'] = os.environ.get('DOCDB_HOST')
        appConfig['dbUsername'] = os.environ.get('DOCDB_USERNAME')
        appConfig['dbPassword'] = os.environ.get('DOCDB_PASSWORD')
        #appConfig['connectionString'] = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false'
        appConfig['connectionString'] = 'mongodb://'+appConfig['dbUsername']+':'+appConfig['dbPassword']+'@'+appConfig['dbHost']+'/?retryWrites=false'

        reporter(appConfig)


if __name__ == "__main__":
    main()
