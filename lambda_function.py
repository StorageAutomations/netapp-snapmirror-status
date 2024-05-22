import pandas as pd
import requests
import boto3
import json
from time import sleep
from time import time

from snap_functions import fsxn

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


def get_snapmirror_status_df(targetClusterDict):
    sprlist=[]
    fieldstring='source.path=*&destination.path=*&policy.name=*&destination.svm.name=*&lag_time=>PT0S&state=*'
    snapString = f"/api/snapmirror/relationships/?{fieldstring}"
    spr=requests.get(targetClusterDict['url']+snapString,
        headers=targetClusterDict['header'],
        verify=False).json()['records']
    timestamp=round(time()*1000)
    for sp in spr:
        sprlist.append(
            [timestamp] +
            sp['source']['path'].split(':') + 
            sp['destination']['path'].split(':') + 
            [sp['policy']['name'], sp['state'], sp['lag_time']]
        )
    columns=['Timestamp','SourceSvm','SourceVol','TargetSvm','TargetVol','SnapMirrorPolicy','State','LagTime']
    sprdf=pd.DataFrame(sprlist, columns=columns)
    
    return sprdf


def lambda_handler(event, context):
    fsx_client = boto3.client('fsx', 'us-west-2')
    secmgr_client = boto3.client('secretsmanager', 'us-west-2')

    fsx=fsx_client.describe_file_systems()['FileSystems']
    fsxlist=[]
    for fs in fsx:
        for t in fs['Tags']:
            if t['Key'] == 'Name':
                print(f"Getting cluster details for {t['Value']}")
                fsxlist.append(
                    {
                        'name':t['Value'], 
                        'fsxnid':fs['FileSystemId'],
                        'ip':fs['OntapConfiguration']['Endpoints']['Management']['IpAddresses'][0],
                        'secret':json.loads(secmgr_client.get_secret_value(
                                        SecretId=f"fsx-{t['Value']}"
                                    )['SecretString'])['fsxadmin']
                    }
                )

    columns=['SourceSvm','SourceVol','TargetSvm','TargetVol','SnapMirrorPolicy','State','LagTime']
    sprdf=pd.DataFrame([], columns=columns)
    for sp in fsxlist:
        print(f"Getting snapmirror details for {sp['name']}, {sp['ip']}")
        targetFSxN=fsxn.getClusterInformation(
            sp['ip'],
            'fsxadmin',
            sp['secret']
        )
        sprdf=pd.concat([sprdf, get_snapmirror_status_df(targetFSxN)])

    sprdflist=[]
    for ind in sprdf.index:
        sprdflist.append({
                "SourceSvm":sprdf['SourceSvm'][ind],
                "SourceVol":sprdf['SourceVol'][ind],
                "TargetSvm":sprdf['TargetSvm'][ind],
                "TargetVol":sprdf['TargetVol'][ind],
                "SnapMirrorPolicy":sprdf['SnapMirrorPolicy'][ind],
                "State":sprdf['State'][ind],
                "LagTime":sprdf['LagTime'][ind]
            })
    
    print(sprdflist)
    return json.loads(json.dumps(sprdflist, default=str))

def main():
    fsx_client = boto3.client('fsx', 'us-west-2')
    secmgr_client = boto3.client('secretsmanager', 'us-west-2')

    fsx=fsx_client.describe_file_systems()['FileSystems']
    fsxlist=[]
    for fs in fsx:
        for t in fs['Tags']:
            if t['Key'] == 'Name':
                # print(f"Getting cluster details for {t['Value']}")
                fsxlist.append(
                    {
                        'name':t['Value'], 
                        'fsxnid':fs['FileSystemId'],
                        'ip':fs['OntapConfiguration']['Endpoints']['Management']['IpAddresses'][0],
                        'secret':json.loads(secmgr_client.get_secret_value(
                                        SecretId=f"fsx-{t['Value']}"
                                    )['SecretString'])['fsxadmin']
                    }
                )

    columns=['Timestamp','SourceSvm','SourceVol','TargetSvm','TargetVol','SnapMirrorPolicy','State','LagTime']
    sprdf=pd.DataFrame([], columns=columns)
    while(True):
        for sp in fsxlist:
            print(f"Tracking snapmirror details for {sp['name']}, {sp['ip']}")
            targetFSxN=fsxn.getClusterInformation(
                sp['ip'],
                'fsxadmin',
                sp['secret']
            )
            sprdf=pd.concat([sprdf, get_snapmirror_status_df(targetFSxN)])

        sprdflist=[]
        for ind in sprdf.index:
            sprdflist.append({
                    "Timestamp":sprdf['Timestamp'][ind],
                    "SourceSvm":sprdf['SourceSvm'][ind],
                    "SourceVol":sprdf['SourceVol'][ind],
                    "TargetSvm":sprdf['TargetSvm'][ind],
                    "TargetVol":sprdf['TargetVol'][ind],
                    "SnapMirrorPolicy":sprdf['SnapMirrorPolicy'][ind],
                    "State":sprdf['State'][ind],
                    "LagTime":sprdf['LagTime'][ind]
                })
        print(sprdf)
        sleep(300)

if __name__ == "__main__":
    main()