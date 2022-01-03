import sys
import json
import pymongo
import os

dbHost = os.environ.get('DOCDB_HOST')
dbUsername = os.environ.get('DOCDB_USERNAME')
dbPassword = os.environ.get('DOCDB_PASSWORD')

if ".docdb." in dbHost:
    #connectionString = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&retryWrites=false'
    connectionString = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?ssl=false&retryWrites=false'
    print('connecting to DocumentDB at {}'.format(dbHost))
else:
    #connectionString = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost+'/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false'
    connectionString = 'mongodb://'+dbUsername+':'+dbPassword+'@'+dbHost
    print('connecting to MongoDB at {}'.format(dbHost))


def shedLoad(opKillDb,opKillMinSeconds):
    client = pymongo.MongoClient(connectionString)

    opDict = client.admin.command("currentOp")["inprog"]
    
    for thisOp in opDict:
        if 'op' in thisOp:
            thisOpNs = "UNKNOWN"
            if 'ns' in thisOp:
                thisOpNs = thisOp['ns']
            
            thisOpSecsRunning = -1
            if 'secs_running' in thisOp:
                thisOpSecsRunning = thisOp['secs_running']
                
            thisOpOpId = -1
            if 'opid' in thisOp:
                thisOpOpId = thisOp['opid']

            thisOpDollarDb = "UNKNOWN"
            if '$db' in thisOp:
                thisOpDollarDb = thisOp['$db']
                
            print("op = {}, $db = {}, ns = {}, secs_running = {}, opid = {}".format(thisOp['op'],thisOpDollarDb,thisOpNs,thisOpSecsRunning,thisOpOpId))
            
            if ((thisOpDollarDb == opKillDb) and (thisOpSecsRunning > opKillMinSeconds)):
                print("  killing opid {}".format(thisOpOpId))
                client.admin.command({ "killOp": 1, "op": thisOpOpId })
            
        else:
            print("*** SKIPPING *** | {}".format(thisOp))


def main():
    opKillDb = sys.argv[1]
    opKillMinSeconds = int(sys.argv[2])
    print("Killing running ops for database {} for operations over {} second(s)".format(opKillDb,opKillMinSeconds))
    shedLoad(opKillDb,opKillMinSeconds)


if __name__ == "__main__":
    main()
