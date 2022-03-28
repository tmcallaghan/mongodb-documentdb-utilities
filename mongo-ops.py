import argparse
from datetime import datetime, timedelta
from pathlib import Path
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
    
    print('connecting to MongoDB aliased as {}'.format(appConfig['serverAlias']))
    client = pymongo.MongoClient(appConfig['uri'])

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
        json.dump(finalDict, fp, indent=appConfig['jsonIndent'], default=str)
        
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
    f1UptimeDays = f1UptimeHours / 24

    if False:
        # show metrics based merely on file1 uptime and metrics
        print('Calculating metrics from start file and uptime of {} {}(s)'.format(f1UptimeHours,appConfig['unitOfMeasure']))
        print('')
        
        if appConfig['unitOfMeasure'] == 'sec':
            useTime = f1UptimeSeconds
        elif appConfig['unitOfMeasure'] == 'min':
            useTime = f1UptimeMinutes
        elif appConfig['unitOfMeasure'] == 'hr':
            useTime = f1UptimeHours
        else:
            useTime = f1UptimeDays
        
        printEvalHeader(appConfig['unitOfMeasure'])
        printEval('opCounters','',useTime,dict1Start['opcounters']['query'],dict1Start['opcounters']['insert'],dict1Start['opcounters']['update'],dict1Start['opcounters']['delete'])
        printEval('docCounters','',useTime,0,dict1Start['docmetrics']['inserted'],dict1Start['docmetrics']['updated'],dict1Start['docmetrics']['deleted'])
        
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
        f2UptimeDays = f2UptimeHours / 24
        
        if appConfig['unitOfMeasure'] == 'sec':
            useTime = f2UptimeSeconds
        elif appConfig['unitOfMeasure'] == 'min':
            useTime = f2UptimeMinutes
        elif appConfig['unitOfMeasure'] == 'hr':
            useTime = f2UptimeHours
        else:
            useTime = f2UptimeDays
        
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
        printEvalHeader(appConfig['unitOfMeasure'],dbColumnWidth,collColumnWidth)
        printEval('opCounters','',useTime,dict2Start['opcounters']['query'] - dict1Start['opcounters']['query'],
                                          dict2Start['opcounters']['insert'] - dict1Start['opcounters']['insert'],
                                          dict2Start['opcounters']['update'] - dict1Start['opcounters']['update'],
                                          dict2Start['opcounters']['delete'] - dict1Start['opcounters']['delete'],
                                          dbColumnWidth,collColumnWidth)
        printEval('docCounters','',useTime,0,dict2Start['docmetrics']['inserted'] - dict1Start['docmetrics']['inserted'],
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
                printEval(thisDb,thisColl,useTime,endCollDict['search calls'] - startCollDict['search calls'],
                                                  endCollDict['insert calls'] - startCollDict['insert calls'],
                                                  #endCollDict['update calls'] - startCollDict['update calls'],
                                                  endCollDict.get('modify calls',0) - startCollDict.get('modify calls',0),
                                                  endCollDict['remove calls'] - startCollDict['remove calls'],
                                                  dbColumnWidth,collColumnWidth)
        

def printEvalHeader(unitOfMeasure,dbColumnWidth,collColumnWidth):
    print('{:<{dbWidth}s} | {:<{collWidth}s} | {:>12s} | {:>12s} | {:>12s} | {:>12s}'.format('Database','Collection','Query/'+unitOfMeasure,'Insert/'+unitOfMeasure,'Update/'+unitOfMeasure,'Delete/'+unitOfMeasure,dbWidth=dbColumnWidth,collWidth=collColumnWidth))
    print('----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')


def printEval(thisLabel1,thisLabel2,thisTime,thisQuery,thisInsert,thisUpdate,thisDelete,dbColumnWidth,collColumnWidth):
    if (thisTime == 0):
        myQps = 0
        myIps = 0
        myUps = 0
        myDps = 0
    else:
        myQps = max(thisQuery / thisTime,0)
        myIps = max(thisInsert / thisTime,0)
        myUps = max(thisUpdate / thisTime,0)
        myDps = max(thisDelete / thisTime,0)
    #print('{:<{dbWidth}s} | {:<{collWidth}s} | {:12,.0f} | {:12,.0f} | {:12,.0f} | {:12,.0f}'.format(thisLabel1,thisLabel2,thisQuery,thisInsert,thisUpdate,thisDelete,dbWidth=dbColumnWidth,collWidth=collColumnWidth))
    print('{:<{dbWidth}s} | {:<{collWidth}s} | {:12,.0f} | {:12,.0f} | {:12,.0f} | {:12,.0f}'.format(thisLabel1,thisLabel2,myQps,myIps,myUps,myDps,dbWidth=dbColumnWidth,collWidth=collColumnWidth))


def main():
    parser = argparse.ArgumentParser(description='Dump and restore indexes from MongoDB to DocumentDB.')
        
    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')
    
    parser.add_argument('--uri',
                        required=False,
                        type=str,
                        help='MongoDB Connection URI')

    parser.add_argument('--server-alias',
                        required=False,
                        type=str,
                        help='Alias for server, used to name output file')
                        
    parser.add_argument('--json-indent',
                        required=False,
                        type=int,
                        default=0,
                        help='Number of spaces for JSON indententation')
                        
    parser.add_argument('--collect',
                        required=False,
                        action='store_true',
                        help='Collect data from a running MongoDB server.')

    parser.add_argument('--compare',
                        required=False,
                        action='store_true',
                        help='Compare data from two executions of --collect on the same server.')

    parser.add_argument('--file1',
                        required=False,
                        type=str,
                        help='First file for comparison process.')

    parser.add_argument('--file2',
                        required=False,
                        type=str,
                        help='Second file for comparison process.')
    
    parser.add_argument('--unit-of-measure',
                        required=False,
                        default='day',
                        help='Unit of measure for reporting [sec | min | hr | day]')

    args = parser.parse_args()
    
    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    if (not (args.collect or args.compare)) or (args.collect and args.compare):
        message = "must specify one of --collect or --compare"
        parser.error(message)

    if args.unit_of_measure not in ['sec','min','hr','day']:
        message = "--unit-of-measure must be one of ['sec','min','hr','day'] for second, minute, hour, day."
        parser.error(message)
        
    # if collect, require --uri and --server-alias
    if args.collect and (args.uri is None):
        message = "must specify --uri"
        parser.error(message)
    if args.collect and (args.server_alias is None):
        message = "must specify --server-alias"
        parser.error(message)
    
    # if compare, require --file1
    if args.compare and (args.file1 is None):
        message = "must specify --file1"
        parser.error(message)
    # test for existence of --file1
    if args.compare and not Path(args.file1).exists():
        message = "file {} does not exist".format(args.file1)
        parser.error(message)
    # if compare, require --file2
    if args.compare and (args.file2 is None):
        message = "must specify --file2"
        parser.error(message)
    # test for existence of --file2
    if args.compare and not Path(args.file2).exists():
        message = "file {} does not exist".format(args.file2)
        parser.error(message)

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['serverAlias'] = args.server_alias
    appConfig['numRunSeconds'] = 0
    appConfig['numSecondsFeedback'] = 0
    appConfig['jsonIndent'] = args.json_indent
    appConfig['file1'] = args.file1
    appConfig['file2'] = args.file2
    appConfig['numFiles'] = 2
    appConfig['unitOfMeasure'] = args.unit_of_measure
    
    if args.collect:
        mongoCollect(appConfig)
        
    elif args.compare:
        print('')
        mongoEvaluate(appConfig)
        print('')


if __name__ == "__main__":
    main()
