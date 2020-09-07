from flask import Flask
import pyodbc
import json

from flask import request


app = Flask(__name__)

    
conn = pyodbc.connect('Driver={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.0.so.1.1};'
                      'Server=dqassqlserver.database.windows.net;'
                      'Database=dqassqldb;'
					  'UID=uilogin;'
					  'PWD=Genpact@123;'
                      'Trusted_Connection=no;')
cursor = conn.cursor()  

@app.route('/dqservices/metadata/v1/<id>',methods=['GET'])
def getmetadata(id): 
 storedProc = "DQS.USP_GetColumnMetadataBasedOnRowID "+id

 cursor.execute(storedProc)

 recs=cursor.fetchall()
 print(recs)

 itemsdata = []
 for row in recs:
        row0= (str(row[0]))
        itemsdata.append({'SERVERNAME' :row[0], 'SOURCETYPE' :( row[1]), 'DATABASENAME' :( row[2]), 'TABLENAME' :( row[3]), 'COLUMNMETADATAID' :( row[4]), 'SOURCETABLEID' :( row[5]), 'COLUMNNAME' :( row[6]), 'COLUMNORDINALPOSITION' :( row[7]), 'DEFAULTVALUE' :( row[8]), 'ISNULLABLE' :( row[9]), 'DATATYPE' :( row[10]), 'STRINGLENGTH' :( row[11]), 'NUMERICLENGTH' :( row[12]), 'ISPRIMARY' :( row[13]), 'ISALPHANUMERIC' :( row[14]), 'REGEX' :( row[15]), 'ISSENSITIVECOLUMN' :( row[16]), 'ISMANDATORY' :( row[17]), 'IGNOREVALIDATION' :( row[18])})

 itemsobj=({"metadata":itemsdata})
 data= json.dumps(itemsobj)
 return(data)



@app.route('/dqservices/metadata/v1',methods=['POST'])

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
