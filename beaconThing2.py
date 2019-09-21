

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import time
import json
import csv
from datetime import datetime

# A random programmatic shadow client ID.
SHADOW_CLIENT = "myShadowClient"
HOST_NAME = "acn0rrcuyylct-ats.iot.us-east-2.amazonaws.com"
ROOT_CA = "RootCA1.pem.txt"
PRIVATE_KEY = "0fe620e940-private.pem.key"
CERT_FILE = "0fe620e940-certificate.pem.crt"
# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "A0179720Y"
# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback(payload, responseStatus, token):
	print()
	print('UPDATE: $aws/things/' + SHADOW_HANDLER + '/shadow/update/#')
	print("payload = " + payload)
	print("responseStatus = " + responseStatus)
	print("token = " + token)
# Create, configure, and connect a shadow client.
myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME, 8883)
myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY,
CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()
# Create a programmatic representation of the shadow.
myDeviceShadow = myShadowClient.createShadowHandlerWithName(SHADOW_HANDLER, True)
filename = 'Data_Beacon_Thing1.csv'

##create a dictionary that stores the data from the csv file
Beacon = {'location':[],'Sensor1':[],'Sensor2':[],'Sensor3':[],'Sensor4':[],'Sensor5':[],'Sensor6':[],'Sensor7':[],'Sensor8':[],'Sensor9':[],'Sensor10':[],'Sensor11':[],'Sensor12':[],'Sensor13':[]}
with open(filename,'r') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader)
    for row in csvreader:
        Beacon['location'].append(row[0])
        Beacon['Sensor1'].append(row[2])
        Beacon['Sensor2'].append(row[3])
        Beacon['Sensor3'].append(row[4])
        Beacon['Sensor4'].append(row[5])
        Beacon['Sensor5'].append(row[6])
        Beacon['Sensor6'].append(row[7])
        Beacon['Sensor7'].append(row[8])
        Beacon['Sensor8'].append(row[9])
        Beacon['Sensor9'].append(row[10])
        Beacon['Sensor10'].append(row[11])
        Beacon['Sensor11'].append(row[12])
        Beacon['Sensor12'].append(row[13])
        Beacon['Sensor13'].append(row[14])

Length = len(Beacon['location'])
matric = "A0179720Y"
for i in range(0,Length):
    CurrentTime = datetime.now()
    TimeStmp =  str(datetime.utcnow())
    sendData = {
            "state":
                {"reported":
                    {
                            "id":"BeaconThing2",
                            "Timestamp":TimeStmp,
                            "MatID" :matric,
                            "Loc":str(Beacon['location'][i]),
                            "S1":str(Beacon['Sensor1'][i]),
                            "S2":str(Beacon['Sensor2'][i]),
                            "S3":str(Beacon['Sensor3'][i]),
                            "S4":str(Beacon['Sensor4'][i]),
                            "S5":str(Beacon['Sensor4'][i]),
                            "S6":str(Beacon['Sensor6'][i])
                                                       
                            
                            }
                    }
                }
    Message = json.dumps(sendData)
    myDeviceShadow.shadowUpdate(Message, myShadowUpdateCallback,5)
    time.sleep(5)