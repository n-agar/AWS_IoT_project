from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import time
import json
import csv
from datetime import datetime

# A random programmatic shadow client ID.
SHADOW_CLIENT = "myShadowClient1"
HOST_NAME = "acn0rrcuyylct-ats.iot.us-east-2.amazonaws.com"
ROOT_CA = "RootCA1.pem.txt"
PRIVATE_KEY = "986f37ca57-private.pem.key"
CERT_FILE = "986f37ca57-certificate.pem.crt"
SHADOW_HANDLER = "BeaconThing"
def myShadowUpdateCallback(payload, responseStatus, token):
	print()
	print('UPDATE: $aws/things/' + SHADOW_HANDLER + '/shadow/update/#')
	print("payload = " + payload)
	print("responseStatus = " + responseStatus)
	print("token = " + token)
# Create, configure, and connect a shadow client.
myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME, 8883)
myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY,CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()
# Create a programmatic representation of the shadow.
myDeviceShadow = myShadowClient.createShadowHandlerWithName(SHADOW_HANDLER, True)
filename = 'Beacon_Data_ThingNew.csv'

##create a dictionary that stores the data from the csv file
Beacon2 = {'location':[],'Sensor1':[],'Sensor2':[],'Sensor3':[],'Sensor4':[],'Sensor5':[],'Sensor6':[],'Sensor7':[],'Sensor8':[],'Sensor9':[],'Sensor10':[],'Sensor11':[],'Sensor12':[],'Sensor13':[]}
with open(filename,'r') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader)
    for row in csvreader:
        Beacon2['location'].append(row[0])
        Beacon2['Sensor1'].append(row[2])
        Beacon2['Sensor2'].append(row[3])
        Beacon2['Sensor3'].append(row[4])
        Beacon2['Sensor4'].append(row[5])
        Beacon2['Sensor5'].append(row[6])
        Beacon2['Sensor6'].append(row[7])
        Beacon2['Sensor7'].append(row[8])
        Beacon2['Sensor8'].append(row[9])
        Beacon2['Sensor9'].append(row[10])
        Beacon2['Sensor10'].append(row[11])
        Beacon2['Sensor11'].append(row[12])
        Beacon2['Sensor12'].append(row[13])
        Beacon2['Sensor13'].append(row[14])

Length = len(Beacon2['location'])
matric = "A0179720Y"
for i in range(0,Length):
    ##get the current time and time stamp
    CurrentTime = datetime.now()
    TimeStmp =  str(datetime.utcnow())
    sendData = {
            "state":
                {"reported":
                    {
                            "id":"BeaconData",
                            "Timestamp":TimeStmp,
                            "MatID" :matric,
                            "Loc":str(Beacon2['location'][i]),
                            "S1":str(Beacon2['Sensor1'][i]),
                            "S2":str(Beacon2['Sensor2'][i]),
                            "S3":str(Beacon2['Sensor3'][i]),
                            "S4":str(Beacon2['Sensor4'][i]),
                            "S5":str(Beacon2['Sensor4'][i]),
                            "S6":str(Beacon2['Sensor6'][i])
                                                       
                            
                            }
                    }
                }

    Message = json.dumps(sendData)
    myDeviceShadow.shadowUpdate(Message, myShadowUpdateCallback,5)
    time.sleep(5)
