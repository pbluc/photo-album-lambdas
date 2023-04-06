from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import json
import boto3
import inflection

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
    client = boto3.client('lexv2-runtime')
    
    #print(event)
    query = event["queryStringParameters"]['q']
    
    response = client.recognize_text(
        botId='GIZSBALPE5',
        botAliasId='HSJKRMSPVA',
        localeId='en_US',
        sessionId='testuser',
        text=query)
    #print(response)
    
    results = []
    
    intent = response['sessionState']['intent']['name']
    #print(intent)
    if intent == 'SearchIntent':
        slots = response['sessionState']['intent']['slots']
        
        search_terms = []
        search_terms.append(inflection.singularize(slots['queryTerm1']['value']['originalValue']).lower())
        if slots['queryTerm2'] is not None:
            search_terms.append(inflection.singularize(slots['queryTerm2']['value']['originalValue']).lower())
        
        #print(search_terms)
        search = get_opensearch()
        search_query = {
            "query": {
                "terms_set": {
                    "labels": {
                        "terms": search_terms,
                        "minimum_should_match_script": {
                            "source": "params.num_terms"
                        }
                    }
                }
            }
        }
        search_response = search.search(index=INDEX, body=search_query)
        #print(search_response)
        
        hits = search_response['hits']['hits']
        for hit in hits:
            bucket = hit['_source']['bucket']
            key = hit['_source']['objectKey']
            labels = hit['_source']['labels']
            
            url = boto3.client('s3').generate_presigned_url(
                ClientMethod='get_object', 
                Params={'Bucket': bucket, 'Key': key},
                ExpiresIn=3600)
            
            results.append({
                'url': url,
                'labels': labels
            })
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps({'results': results})
    }
