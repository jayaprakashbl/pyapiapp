# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 19:19:05 2020

@author: 703239259
"""

from flask import Flask, jsonify
import pyodbc
import json

from flask import request


app = Flask(__name__)

    
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=dqassqlserver.database.windows.net;'
                      'Database=dqassqldb;'
					  'UID=uilogin;'
					  'PWD=Genpact@123;'
                      'Trusted_Connection=no;')
cursor = conn.cursor()  

#Get metadata based on id
@app.route('/dqservices/metadata/v1/<id>',methods=['GET'], endpoint='getmetadata')
def getmetadata(id): 
 storedProc = "DQS.USP_GetColumnMetadataBasedOnRowID "+id
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'servername' :row[0], 'sourcetype' :( row[1]), 'databasename' :( row[2]), 'tablename' :( row[3]), 'columnmetadataid' :( row[4]), 'sourcetableid' :( row[5]), 'columnname' :( row[6]), 'columnordinalposition' :( row[7]), 'defaultvalue' :( row[8]), 'isnullable' :( row[9]), 'datatype' :( row[10]), 'stringlength' :( row[11]), 'numericlength' :( row[12]), 'isprimary' :( row[13]), 'isalphanumeric' :( row[14]), 'regex' :( row[15]), 'issensitivecolumn' :( row[16]), 'ismandatory' :( row[17]), 'ignorevalidation' :( row[18])})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)


#Update metadata
@app.route('/dqservices/metadata/v1',methods=['POST'], endpoint='updatemetadata')
def updatemetadata():
 metadata = request.get_json("force = True")
 columnmetadataid = metadata['columnmetadataid']
 isprimary = metadata['isprimary']
 isalphanumeric = metadata['isalphanumeric']
 regex = metadata['regex']
 issensitivecolumn = metadata['issensitivecolumn']
 ismandatory = metadata['ismandatory']
 ignorevalidation = metadata['ignorevalidation']
 sql = " EXEC DQS.USP_UpdateColumnMetadataBasedOnRowID @columnmetadataid=?, @isprimary=?, @isalphanumeric=?, @regex=?, @issensitivecolumn=?, @ismandatory=?, @ignorevalidation=?"
 params = (columnmetadataid,isprimary, isalphanumeric,regex,issensitivecolumn,ismandatory,ignorevalidation)
 cursor.execute(sql, params)
 cursor.commit()
 response = jsonify({"message":"updated the metadata successfully"})
 return response


#Get column metadata based on tablename
@app.route('/dqservices/cmetadatadata/v1/<id>',methods=['GET'], endpoint='getcolumnmetadatabasedontablename')
def getcolumnmetadatabasedontablename(id): 
 storedProc = "DQS.[USP_GetColumnMetadataBasedOnTablename]"+id
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 print(recs)
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'columnmetadataid' : row[0] ,'sourcetableid' : row[1] ,'columnname' : row[2] ,'columnordinalposition' : row[3] ,'defaultvalue' : row[4] ,'isnullable' : row[5] ,'datatype' : row[6] ,'stringlength' : row[7] ,'numericlength' : row[8] ,'isprimary' : row[9] ,'isalphanumeric' : row[10] ,'regex' : row[11] ,'issensitivecolumn' : row[12] ,'ismandatory' : row[13] ,'ignorevalidation' : row[14] })
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)


#Get the databasename
@app.route('/dqservices/databasename/v1/all/<servername>',methods=['GET'], endpoint='getdatabasename')
def getdatabasename(servername): 
 storedProc = "DQS.[USP_GetDataBase] "+servername
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'databasename' :row[0]})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)


#Get the servername
@app.route('/dqservices/servername/v1/all/<sourcetype>',methods=['GET'], endpoint='getservername')
def getservername(sourcetype): 
 storedProc = "DQS.[USP_GetServerName] "+sourcetype
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'servername' :row[0]})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)

#Get the sourcetype
@app.route('/dqservices/sourcetype/v1/all',methods=['GET'], endpoint='getsourcetype')
def getsourcetype(): 
 storedProc = "DQS.[USP_GetSourceType]"
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'sourcetype' :row[0]})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)


#Get the tablename
@app.route('/dqservices/tablename/v1/all/<dbname>',methods=['GET'], endpoint='gettablename')
def gettablename(dbname): 
 storedProc = "DQS.[USP_GetTableName] "+dbname
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'tableid': row[0], 'tablename' :row[1]})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)
 

#Get the businessrules based on source table id
@app.route('/dqservices/businessrulesonsourcetableid/v1/<sourcetableid>',methods=['GET'], endpoint='getbusinessrulesonsourcetableid')
def getobusinessrules(sourcetableid):
 storedProc = "DQS.[USP_GetBusinessRulesBasedOnSourceTableID] "+sourcetableid
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'businessruleid' :row[0],'sourcetableid' :( row[1]), 'tablename' :( row[2]), 'dqdimension' :( row[3]), 'businessrulename' :( row[4]), 'businessrulelogic' :( row[5]), 'logdatetime' :( row[6]), 'isactive' :( row[7]), 'jointable1' :( row[8]), 'jointable2' :( row[9]), 'jointable3' :( row[10]), 'jointable4' :( row[11]), 'sendemail' :( row[12]), 'emailid' :( row[13]), 'actionpoints' :( row[14]), 'columnname' :( row[15])})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)

#Get the businessrules based on business rule id
@app.route('/dqservices/obusinessrulesid/v1/<businessruleid>',methods=['GET'], endpoint='getobusinessrulesonruleid')
def getobusinessrules(businessruleid):
 storedProc = "DQS.[USP_GetBusinessRulesBasedOnRowID] "+ businessruleid
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'tablename' :row[0],'businessruleid' :( row[1]), 'sourcetableid' :( row[2]), 'dqdimensionid' :( row[3]), 'businessrulename' :( row[4]), 'businessrulelogic' :( row[5]), 'businessruledesc' :( row[6]), 'logdatetime' :( row[7]), 'columnname' :( row[8]), 'isactive' :( row[9]), 'tablejobid1' :( row[10]), 'tablejobid2' :( row[11]), 'tablejobid3' :( row[12]), 'tablejobid4' :( row[13]), 'sendemail' :( row[14]), 'emailid' :( row[15]), 'actionpoints' :( row[16])})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)

#Update the businessrules
@app.route('/dqservices/ubusinessrules/v1',methods=['POST'], endpoint='updatebusinessrules')
def updatebusinessrules():
 businessrules = request.get_json("force = true")
 businessruleid = businessrules['businessruleid']
 businessrulename = businessrules['businessrulename']
 businessrulelogic = businessrules['businessrulelogic']
 businessruledesc = businessrules['businessruledesc']
 tablejobid1 = businessrules['tablejobid1']
 tablejobid2 = businessrules['tablejobid2']
 tablejobid3 = businessrules['tablejobid3']
 tablejobid4 = businessrules['tablejobid4']
 isactive = businessrules['isactive']
 sendemail = businessrules['sendemail']
 emailid = businessrules['emailid']
 actionpoints = businessrules['actionpoints'] 
 sourcetableid = businessrules['sourcetableid']
 columnmetadataid = businessrules['columnmetadataid'] 

 sql = " EXEC DQS.USP_UpdateBusinessRuleBasedOnRowID @businessruleid=?, @businessrulename=?, @businessrulelogic=?, @businessruledesc=?, @tablejobid1=?, @tablejobid2=?, @tablejobid3=?, @tablejobid4=?,  @isactive=?, @sendemail=?, @emailid=?, @actionpoints=?, @sourcetableid=?, @columnmetadataid=? "
 params = (businessruleid,businessrulename,businessrulelogic,businessruledesc,tablejobid1,tablejobid2,tablejobid3,tablejobid4,isactive,sendemail,emailid,actionpoints,sourcetableid,columnmetadataid )
 cursor.execute(sql, params)
 cursor.commit()
 response = jsonify({"message":"updated the businessrules successfully"})
 return response


#Insert the businessrules
@app.route('/dqservices/ibusinessrules/v1',methods=['POST'], endpoint='insertbusinessrules')
def updatebusinessrules():
 businessrules = request.get_json("force = True")
 dqdimensionid = businessrules['dqdimensionid']
 businessrulename = businessrules['businessrulename']
 businessrulelogic = businessrules['businessrulelogic']
 businessruledesc = businessrules['businessruledesc']
 isactive = businessrules['isactive']
 tablejobid1 = businessrules['tablejobid1']
 tablejobid2 = businessrules['tablejobid2']
 tablejobid3 = businessrules['tablejobid3']
 tablejobid4 = businessrules['tablejobid4']
 sendemail = businessrules['sendemail']
 emailid = businessrules['emailid']
 actionpoints = businessrules['actionpoints']
 sourcetableid = businessrules['sourcetableid']
 columnmetadataid = businessrules['columnmetadataid']
   
 sql = " EXEC DQS.[USP_InsertBusinessRules]  @dqdimensionid=?, @businessrulename=?, @businessrulelogic=?, @businessruledesc=?, @isactive=?, @sendemail=?, @tablejobid1=?, @tablejobid2=?, @tablejobid3=?, @tablejobid4=?, @emailid=?, @actionpoints=?, @sourcetableid=?, @columnmetadataid=? "

 params = (dqdimensionid,businessrulename,businessrulelogic,businessruledesc,isactive,tablejobid1,tablejobid2,tablejobid3,tablejobid4,sendemail,emailid,actionpoints,sourcetableid,columnmetadataid)
 cursor.execute(sql, params)
 cursor.commit()
 response = jsonify({"message":"inserted the businessrules successfully"})
 return response

#Get tableobjectid for businessrules
@app.route('/dqservices/tableobjectid/v1/all',methods=['GET'], endpoint='tableobjectid')
def gettableobjectid():
 storedProc = "DQS.[USP_GetTableJobID]"
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'tablejobid' :row[0],'objectName' :( row[1])})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)

#Get dqdimensionid for businessrules
@app.route('/dqservices/dqdimensionid/v1/all',methods=['GET'], endpoint='dqdimensionid')
def getdqdimensionid():
 storedProc = "DQS.[USP_GetDimensionID]"
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'dimensionid' :row[0], 'dimensionname' : (row[1])})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)

#Get all the sourcetablemaster
@app.route('/dqservices/sourcetablemaster/v1/all',methods=['GET'], endpoint='getsourcetablemaster')
def getsourcetablemaster():
    storedProc = "DQS.[USP_GetSourceTableInfo]" 
    cursor.execute(storedProc)
    recs=cursor.fetchall()
    itemsdata = []
    for row in recs:
            row0= (str(row[0]))
            itemsdata.append({'sourcetableid' :row[0],'sourcetype' :( row[1]), 'servername' :( row[2]), 'databasename' :( row[3]), 'schemaname' :( row[4]), 'tablename' :( row[5]), 'sourcecredentails' :( row[6]), 'sourcefilepath' :( row[7]), 'isactive' :( row[8]), 'frequencytype' :(row[9]), 'dayofweek' :(row[10]), 'timings' : (row[11])})
    itemsobj=({"output":itemsdata})
    data= json.dumps(itemsobj)
    return(data)

#Get the sourcetablemaster information based on id
@app.route('/dqservices/sourcetablemaster/v1/<sourcetableid>',methods=['GET'], endpoint='getsourcetablemasterbasedonid')
def getsourcetablemasteronid(sourcetableid):
    storedProc = "DQS.[USP_GetSourceTableInfoBasedOnID]" + sourcetableid
    cursor.execute(storedProc)
    recs=cursor.fetchall()
    itemsdata = []
    for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'sourcetableid' :row[0],'sourcetype' :( row[1]), 'servername' :( row[2]), 'databasename' :( row[3]), 'schemaname' :( row[4]), 'tablename' :( row[5]), 'sourcecredentails' :( row[6]), 'sourcefilepath' :( row[7]), 'isactive' :( row[8]), 'frequencytype' :(row[9]), 'dayofweek' :(row[10]), 'timings' : (row[11])})
    itemsobj=({"output":itemsdata})
    data= json.dumps(itemsobj)
    return(data)



#Insert the sourcetablemaster
@app.route('/dqservices/isourcetablemaster/v1',methods=['POST'], endpoint='insertsourcetablemaster')
def insertsourcetablemaster():
    sourcetablemaster = request.get_json("force = True")
    sourcetype = sourcetablemaster['sourcetype']
    servername = sourcetablemaster['servername']
    databasename = sourcetablemaster['databasename']
    schemaname = sourcetablemaster['schemaname']
    tablename = sourcetablemaster['tablename']
    sourcecredentails = sourcetablemaster['sourcecredentails']
    sourcefilepath = sourcetablemaster['sourcefilepath']
    isactive = sourcetablemaster['isactive']
    frequencytype = sourcetablemaster['frequencytype']
    dayofweek = sourcetablemaster['dayofweek']
    timings = sourcetablemaster['timings']
    sql = " EXEC DQS.[USP_Insertsourcetablemaster]  @sourcetype=?, @servername=?, @databasename=?, @schemaname=?, @tablename=?, @sourcecredentails=?, @sourcefilepath=?, @isactive=?, @frequencytype=?, @dayofweek=?, @timings=? "
    params = (sourcetype,servername,databasename,schemaname,tablename,sourcecredentails,sourcefilepath,isactive,frequencytype,dayofweek,timings)
    cursor.execute(sql, params)
    cursor.commit()
    response = jsonify({"message":"inserted the sourcetablemaster successfully"})
    return response

#Update the sourcetablemaster
@app.route('/dqservices/usourcetablemaster/v1',methods=['POST'], endpoint='updatesourcetablemaster')
def updatesourcetablemaster():
    sourcetablemaster = request.get_json("force = True")
    sourcetableid = sourcetablemaster['sourcetableid']
    sourcetype = sourcetablemaster['sourcetype']
    servername = sourcetablemaster['servername']
    databasename = sourcetablemaster['databasename']
    schemaname = sourcetablemaster['schemaname']
    tablename = sourcetablemaster['tablename']
    sourcecredentails = sourcetablemaster['sourcecredentails']
    sourcefilepath = sourcetablemaster['sourcefilepath']
    isactive = sourcetablemaster['isactive']
    frequencytype = sourcetablemaster['frequencytype']
    dayofweek = sourcetablemaster['dayofweek']
    timings = sourcetablemaster['timings']
    sql = " EXEC DQS.[USP_UpdateSourceTableMaster] @sourcetableid=?, @sourcetype=?, @servername=?, @databasename=?, @schemaname=?, @tablename=?, @sourcecredentails=?, @sourcefilepath=?, @isactive=?, @frequencytype=?, @dayofweek=?, @timings=? "
    params = (sourcetableid,sourcetype,servername,databasename,schemaname,tablename,sourcecredentails,sourcefilepath,isactive,frequencytype,dayofweek,timings )
    cursor.execute(sql, params)
    cursor.commit()
    response = jsonify({"message":"updated the sourcetablemaster successfully"})
    return response

#Get consolidatedrules based on columnname
@app.route('/dqservices/consolidatedrules/v1/<sourcetableid>',methods=['GET'], endpoint='getconsolidatedrulesbasedonsourcetableid')
def getconsolidatedrulesbasedonsourcetableid(sourcetableid):
    storedProc = "DQS.[USP_GetConsolidatedRulesBasedOnSourceTable]" + sourcetableid
    cursor.execute(storedProc)
    recs=cursor.fetchall()
    itemsdata = []
    for row in recs:
            row0= (str(row[0]))
            itemsdata.append({'columnname' :  row[0],'isnullable' :  row[1],'isprimary' :  row[2],'regex' :  row[3],'issensitivecolumn' :  row[4],'ismandatory' :  row[5],'ignorevalidation' :  row[6],'businessrulename' :  row[7],'businessrulelogic' :  row[8],'isactive' :  row[9],'sendemail' :  row[10],'emailid' :  row[11],'actionpoints' :  row[12]})
    itemsobj=({"output":itemsdata})
    data= json.dumps(itemsobj)
    return(data)


if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)