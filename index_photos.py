from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import json
import boto3

REGION = 'us-east-1'
HOST = 'search-photos-cvhffhoutli3p5mqdo3jnsdkra.us-east-1.es.amazonaws.com'
INDEX = 'photos'

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)

def get_opensearch():
    return OpenSearch(
        hosts=[{'host': HOST, 'port': 443}],
        http_auth=get_awsauth(REGION, 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    rek = boto3.client('rekognition')
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    photo = event['Records'][0]['s3']['object']['key']
    #print(photo)
    
    response = rek.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}})
    
    obj_summary = s3.head_object(Bucket=bucket, Key=photo)
    #print(obj_summary)
    created_timestamp = obj_summary['LastModified']
    
    labels = []
    if 'customlabels' in obj_summary['Metadata']:
        custom_labels = obj_summary['Metadata']['customlabels'].split(",")
        labels = [customLabel.lower() for customLabel in custom_labels]
        #print(labels)
        
    for label in response['Labels']:
        labels.append(label['Name'].lower())
    #print(labels)
    
    document = {
        'objectKey': photo,
        'bucket': bucket, 
        'createdTimestamp': created_timestamp,
        'labels': labels
    }
    
    search = get_opensearch()
    search.index(
        index=INDEX,
        id=photo,
        body=document
    )
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps({})
    }
