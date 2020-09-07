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

@app.route('/dqservices/metadata/v1/all/',methods=['GET'], endpoint='getallmetadata')
def getallmetadata(): 
 storedProc = "DQS.[USP_GetColumnMetadata]"
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'servername' :row[0], 'sourcetype' :( row[1]), 'databasename' :( row[2]), 'tablename' :( row[3]), 'columnmetadataid' :( row[4]), 'sourcetableid' :( row[5]), 'columnname' :( row[6]), 'columnordinalposition' :( row[7]), 'defaultvalue' :( row[8]), 'isnullable' :( row[9]), 'datatype' :( row[10]), 'stringlength' :( row[11]), 'numericlength' :( row[12]), 'isprimary' :( row[13]), 'isalphanumeric' :( row[14]), 'regex' :( row[15]), 'issensitivecolumn' :( row[16]), 'ismandatory' :( row[17]), 'ignorevalidation' :( row[18])})
 itemsobj=({"metadata":itemsdata})
 data= json.dumps(itemsobj)
 return(data)



@app.route('/dqservices/metadata/v1/<id>',methods=['GET'], endpoint='getmetadata')
def getmetadata(id): 
 storedProc = "DQS.USP_GetColumnMetadataBasedOnRowID "+id
 cursor.execute(storedProc)
 recs=cursor.fetchall()
 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'servername' :row[0], 'sourcetype' :( row[1]), 'databasename' :( row[2]), 'tablename' :( row[3]), 'columnmetadataid' :( row[4]), 'sourcetableid' :( row[5]), 'columnname' :( row[6]), 'columnordinalposition' :( row[7]), 'defaultvalue' :( row[8]), 'isnullable' :( row[9]), 'datatype' :( row[10]), 'stringlength' :( row[11]), 'numericlength' :( row[12]), 'isprimary' :( row[13]), 'isalphanumeric' :( row[14]), 'regex' :( row[15]), 'issensitivecolumn' :( row[16]), 'ismandatory' :( row[17]), 'ignorevalidation' :( row[18])})
 itemsobj=({"metadata":itemsdata})
 data= json.dumps(itemsobj)
 return(data)



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

if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)
