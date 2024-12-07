import boto3
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = 'assets'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodbTableName)

getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
deleteMethos = 'DELETE'
healthPath = '/health'
asset = '/asset'
assets = '/assets'

def lambda_handler(event, context)
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
        
def buildResponse(statusCode, body=None):
    response = {
        'statusCode' : statusCode,
        'headers' : {
            'content-Type' : 'application/json',
            'Access-Control-Allow-Origin' : '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body)
    return response