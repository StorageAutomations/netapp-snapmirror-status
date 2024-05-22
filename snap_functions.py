import boto3
import json
import requests
import base64
from time import sleep
import pandas as pd

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

class fsxn():
    def __init__(self, data) -> None:
        super().__init__()
        self.data = data

    def getClusterInformation(clusterAddress,username,password):
        #Dictionary to holder cluster information
        clusterDict = {}

        #Adding URL, usr, and password to the Cluster Dictionary
        clusterDict['url'] = 'https://'+clusterAddress
        AuthBase64String = base64.encodebytes(('%s:%s' % (username, password)).encode()).decode().replace('\n', '')
        clusterDict['header'] = {
            'authorization': "Basic %s" % AuthBase64String
        }

        #String for cluster api call
        clusterString = "/api/cluster"

        #Get Call for cluster information
        clusterNameReq = requests.get(clusterDict['url']+clusterString,
            headers=clusterDict['header'],
            verify=False)
        #catch clusterNameReq.status_code

        #Adding cluster's name to dictionary
        clusterDict['name'] = clusterNameReq.json()['name']

        #String for getting intercluster IP Addresses (Needs to be updated to limit to specific SVM)
        networkIntString = "/api/network/ip/interfaces?services=intercluster-core&fields=ip.address"

        #Get call for IP Addresses
        networkIntReq = requests.get(clusterDict['url']+networkIntString,
            headers=clusterDict['header'],
            verify=False)

        #Adding interfaces to an array in the dictionary
        clusterDict['interfaces'] = []
        for record in networkIntReq.json()['records']:
            clusterDict['interfaces'].append(record['ip']['address'])

        return clusterDict

