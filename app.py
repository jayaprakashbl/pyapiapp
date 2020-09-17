from flask import Flask
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

#Get all metadata
@app.route('/dqservices/metadata/v1/all',methods=['GET'], endpoint='getallmetadata')
def getallmetadata(): 
 storedProc = "DQS.[USP_GetColumnMetadata]"
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'servername' :row[0], 'sourcetype' :( row[1]), 'databasename' :( row[2]), 'tablename' :( row[3]), 'columnmetadataid' :( row[4]), 'sourcetableid' :( row[5]), 'columnname' :( row[6]), 'columnordinalposition' :( row[7]), 'defaultvalue' :( row[8]), 'isnullable' :( row[9]), 'datatype' :( row[10]), 'stringlength' :( row[11]), 'numericlength' :( row[12]), 'isprimary' :( row[13]), 'isalphanumeric' :( row[14]), 'regex' :( row[15]), 'issensitivecolumn' :( row[16]), 'ismandatory' :( row[17]), 'ignorevalidation' :( row[18])})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)


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
 response = "updated the metadata successfully"

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
 

#Get the businessobjectname
@app.route('/dqservices/businessobject/v1/all',methods=['GET'], endpoint='getbusinessobject')
def getbusinessobject():
 storedProc = "DQS.[USP_GetBusinessObject]"
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'businessobjectid' :row[0],'objectname' :( row[1]),'isactive': (row[2])})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)


#Get the businessrules based on objectid
@app.route('/dqservices/obusinessrules/v1/<objectid>',methods=['GET'], endpoint='getobusinessrules')
def getobusinessrules(objectid):
 storedProc = "DQS.[USP_GetBusinessRulesBasedOnObjectID] "+objectid
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'objectName' :row[0],'businessruleid' :( row[1]), 'BusinessObjectID' :( row[2]), 'DQDimensionID' :( row[3]), 'BusinessRuleName' :( row[4]), 'BusinessRuleLogic' :( row[5]), 'BusinessRuleDesc' :( row[6]), 'LogDateTime' :( row[7]), 'ColumnName' :( row[8]), 'IsActive' :( row[9]), 'tablejobid1' :( row[10]), 'tablejobid2' :( row[11]), 'tablejobid3' :( row[12]), 'tablejobid4' :( row[13]), 'SendEmail' :( row[14]), 'EmailId' :( row[15]), 'ActionPoints' :( row[16])})
 itemsobj=({"output":itemsdata})
 data= json.dumps(itemsobj)
 return(data)


#Update the businessrules
@app.route('/dqservices/ubusinessrules/v1',methods=['POST'], endpoint='updatebusinessrules')
def updatebusinessrules():
 businessrules = request.get_json("force = True")
 businessruleid = businessrules['businessruleid']
 businessobjectid = businessrules['businessobjectid']
 businessrulename = businessrules['businessrulename']
 businessrulelogic = businessrules['businessrulelogic']
 businessruledesc = businessrules['businessruledesc']
 columnname = businessrules['columnname']
 isactive = businessrules['isactive']
 sendemail = businessrules['sendemail']
 emailid = businessrules['emailid']
 actionpoints = businessrules['actionpoints']
  
 sql = " EXEC DQS.USP_UpdateBusinessRuleBasedOnRowID @businessruleid=?, @businessobjectid=?, @businessrulename=?, @businessrulelogic=?, @businessruledesc=?, @columnname=?, @isactive=?, @sendemail=?, @emailid=?, @actionpoints=? "

 params = (businessruleid,businessobjectid,businessrulename,businessrulelogic,businessruledesc,columnname,isactive,sendemail,emailid,actionpoints )
 cursor.execute(sql, params)
 cursor.commit()
 response = "updated the businessrules successfully"

 return response


#Insert the businessrules
@app.route('/dqservices/ibusinessrules/v1',methods=['POST'], endpoint='insertbusinessrules')
def updatebusinessrules():
 businessrules = request.get_json("force = True")
 
 businessobjectid = businessrules['businessobjectid']
 dqdimensionid = businessrules['dqdimensionid']
 businessrulename = businessrules['businessrulename']
 businessrulelogic = businessrules['businessrulelogic']
 businessruledesc = businessrules['businessruledesc']
 columnname = businessrules['columnname']
 isactive = businessrules['isactive']
 tablejobid1 = businessrules['tablejobid1']
 tablejobid2 = businessrules['tablejobid2']
 tablejobid3 = businessrules['tablejobid3']
 tablejobid4 = businessrules['tablejobid4']
 sendemail = businessrules['sendemail']
 emailid = businessrules['emailid']
 actionpoints = businessrules['actionpoints']
   
 sql = " EXEC DQS.[USP_InsertBusinessRules]  @businessobjectid=?, @dqdimensionid=?, @businessrulename=?, @businessrulelogic=?, @businessruledesc=?, @columnname=?, @isactive=?, @sendemail=?, @tablejobid1=?, @tablejobid2=?, @tablejobid3=?, @tablejobid4=?, @emailid=?, @actionpoints=? "

 params = (businessobjectid,dqdimensionid,businessrulename,businessrulelogic,businessruledesc,columnname,isactive,tablejobid1,tablejobid2,tablejobid3,tablejobid4,sendemail,emailid,actionpoints )
 cursor.execute(sql, params)
 cursor.commit()
 response = "inserted the businessrules successfully"

 return response


if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)
