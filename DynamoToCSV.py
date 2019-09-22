import csv
import json
import boto3
from operator import itemgetter
import decimal
from boto3.dynamodb.conditions import Key, Attr

#convert dynamodb data to json
class getTableData():
    def default(self,o):
        if(isinstance(o, decimal.Decimal)):
            if (o % 1 >0):
                return(float(o))
            else:
                return(int(o))
        return(super(getTableData,self).default(o))
        

#service client resource created
dynamodb = boto3.resource('dynamodb',aws_access_key_id = <enter the access_key_id>, aws_secret_access_key = <enter the aws_secret_key_id>, region_name = 'us-east-2')

#resource for dynamodb table
tableStore = dynamodb.Table('A0179720Y_Table2') 

##create a variable that stores what data is to be accessed from the table - filter expression
AccessKey = Key('Timestamp').between('2019-09-20 11:03:09.328020','2019-09-21 11:03:09.328020')
## projection attributes to get
AttributesTable = "#date,loc,S1,S2,S3,S4,S5,S6"
## expression attribute
ExpAtrib = {"#date":"Timestamp",}

##gt the data fro dynamo table
data = tableStore.scan(FilterExpresion = AccessKey,ProjectionExpression = AttributesTable, ExpressionAttributeName = ExpAtrib)

storeData = []
SortData = []
                     
#get data from the table 
with open('tabledata.csv','w',newline = '\n') as csvfile:
    writer = csv.writer(csvfile)
    for i in data['Items']:
        inp = json.loads(json.dumps(i, cls=getTableData))
        storeData.append(inp)
        SortData = sorted(storeData,key=itemgetter('Timestamp'))
        writer.writerow(SortData)    
