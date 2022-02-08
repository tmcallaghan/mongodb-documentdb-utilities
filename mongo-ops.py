from datetime import datetime, timedelta
import sys
import json
import pymongo
import time
import os


def mongoCollect(appConfig):
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
    

def mongoEvaluate(appConfig):
    with open(appConfig['file1'], 'r') as fp:
        dict1 = json.load(fp)
    dict1Start = dict1['start']

    f1UptimeSeconds = dict1Start['uptime']
    f1UptimeMinutes = f1UptimeSeconds / 60
    f1UptimeHours = f1UptimeSeconds / 3600

    if False:
        # show metrics based merely on file1 uptime and metrics
        print('Calculating metrics from start file and uptime of {} hour(s)'.format(f1UptimeHours))
        print('')
        printEvalHeader('sec')
        printEval('opCounters','',f1UptimeHours,dict1Start['opcounters']['query'],dict1Start['opcounters']['insert'],dict1Start['opcounters']['update'],dict1Start['opcounters']['delete'])
        printEval('docCounters','',f1UptimeHours,0,dict1Start['docmetrics']['inserted'],dict1Start['docmetrics']['updated'],dict1Start['docmetrics']['deleted'])
        
        for thisDb in dict1Start['collstats']:
            for thisColl in dict1Start['collstats'][thisDb]:
                thisCollDict = dict1Start['collstats'][thisDb][thisColl]
                printEval(thisDb,thisColl,f1UptimeSeconds,thisCollDict['search calls'],thisCollDict['insert calls'],thisCollDict['update calls'],thisCollDict['remove calls'])

    if appConfig['numFiles'] == 2:
        with open(appConfig['file2'], 'r') as fp:
            dict2 = json.load(fp)
        dict2Start = dict2['start']

        f2UptimeSeconds = dict2Start['uptime'] - f1UptimeSeconds
        f2UptimeMinutes = f2UptimeSeconds / 60
        f2UptimeHours = f2UptimeSeconds / 3600

        # determine output column widths
        dbColumnWidth = 12
        collColumnWidth = 12
        for thisDb in dict2Start['collstats']:
            if len(thisDb) > dbColumnWidth:
                dbColumnWidth = len(thisDb)
            for thisColl in dict2Start['collstats'][thisDb]:
                if len(thisColl) > collColumnWidth:
                    collColumnWidth = len(thisColl)
        
        print('')
        print('Calculating metrics from start and end files - duration of {} second(s).'.format(f2UptimeSeconds))
        print('')
        printEvalHeader('sec',dbColumnWidth,collColumnWidth)
        printEval('opCounters','',f1UptimeHours,dict2Start['opcounters']['query'] - dict1Start['opcounters']['query'],
                                                dict2Start['opcounters']['insert'] - dict1Start['opcounters']['insert'],
                                                dict2Start['opcounters']['update'] - dict1Start['opcounters']['update'],
                                                dict2Start['opcounters']['delete'] - dict1Start['opcounters']['delete'],
                                                dbColumnWidth,collColumnWidth)
        printEval('docCounters','',f1UptimeHours,0,dict2Start['docmetrics']['inserted'] - dict1Start['docmetrics']['inserted'],
                                                   dict2Start['docmetrics']['updated'] - dict1Start['docmetrics']['updated'],
                                                   dict2Start['docmetrics']['deleted'] - dict1Start['docmetrics']['deleted'],
                                                   dbColumnWidth,collColumnWidth)
        
        for thisDb in dict2Start['collstats']:
            for thisColl in dict2Start['collstats'][thisDb]:
                endCollDict = dict2Start['collstats'][thisDb][thisColl]
                if (thisDb not in dict1Start['collstats']) or (thisColl not in dict1Start['collstats'][thisDb]):
                    startCollDict = {'search calls':0,'insert calls':0,'update calls':0,'remove calls':0}
                else:
                    startCollDict = dict1Start['collstats'][thisDb][thisColl]
                printEval(thisDb,thisColl,f2UptimeSeconds,endCollDict['search calls'] - startCollDict['search calls'],
                                                          endCollDict['insert calls'] - startCollDict['insert calls'],
                                                          #endCollDict['update calls'] - startCollDict['update calls'],
                                                          endCollDict.get('modify calls',0) - startCollDict.get('modify calls',0),
                                                          endCollDict['remove calls'] - startCollDict['remove calls'],
                                                          dbColumnWidth,collColumnWidth)
        

def printEvalHeader(unitOfMeasure,dbColumnWidth,collColumnWidth):
    print('{:<{dbWidth}s} | {:<{collWidth}s} | {:>12s} | {:>12s} | {:>12s} | {:>12s}'.format('Database','Collection','Query/'+unitOfMeasure,'Insert/'+unitOfMeasure,'Update/'+unitOfMeasure,'Delete/'+unitOfMeasure,dbWidth=dbColumnWidth,collWidth=collColumnWidth))
    print('----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')


def printEval(thisLabel1,thisLabel2,thisTime,thisQuery,thisInsert,thisUpdate,thisDelete,dbColumnWidth,collColumnWidth):
    myQps = max(thisQuery / thisTime,0)
    myIps = max(thisInsert / thisTime,0)
    myUps = max(thisUpdate / thisTime,0)
    myDps = max(thisDelete / thisTime,0)
    print('{:<{dbWidth}s} | {:<{collWidth}s} | {:12,.0f} | {:12,.0f} | {:12,.0f} | {:12,.0f}'.format(thisLabel1,thisLabel2,myQps,myIps,myUps,myDps,dbWidth=dbColumnWidth,collWidth=collColumnWidth))


def main():
    badArguments = False
    
    if (len(sys.argv) == 1):
        # no arguments passed
        badArguments = True
    elif (len(sys.argv) >= 2) and (sys.argv[1].lower() == 'collect'):
        if (len(sys.argv) != 4):
            badArguments = True
    elif (len(sys.argv) >= 2) and (sys.argv[1].lower() == 'evaluate'):
        if (len(sys.argv) not in [3,4]):
            badArguments = True

    if badArguments:
        print("")
        print("usage: python3 mongo-ops.py collect <number-of-seconds-to-run-for> <nickname-for-server>")
        print("          Collect operational data from running MongoDB server")
        print("usage: python3 mongo-ops.py evaluate <file1> [<file2>]")
        print("          Evaluate operational from one or two collected files")
        print("")
        
    elif (sys.argv[1].lower() == 'collect'):
        appConfig = {}
        appConfig['numRunSeconds'] = int(sys.argv[2])
        appConfig['serverAlias'] = sys.argv[3]
        appConfig['numSecondsFeedback'] = 60
        appConfig['dbHost'] = os.environ.get('DOCDB_HOST')
        appConfig['dbUsername'] = os.environ.get('DOCDB_USERNAME')
        appConfig['dbPassword'] = os.environ.get('DOCDB_PASSWORD')
        #appConfig['connectionString'] = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false'
        appConfig['connectionString'] = 'mongodb://'+appConfig['dbUsername']+':'+appConfig['dbPassword']+'@'+appConfig['dbHost']+'/?retryWrites=false'
        mongoCollect(appConfig)

    elif (sys.argv[1].lower() == 'evaluate'):
        appConfig = {}
        appConfig['file1'] = sys.argv[2]
        appConfig['numFiles'] = 1
        print('starting file = {}'.format(appConfig['file1']))
        if (len(sys.argv) == 4):
            appConfig['file2'] = sys.argv[3]
            appConfig['numFiles'] = 2
            print('ending file = {}'.format(appConfig['file2']))
        print('')
        mongoEvaluate(appConfig)
        print('')
        


if __name__ == "__main__":
    main()
