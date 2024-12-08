import boto3
import json
from custome_encoder import CustomeEncoder
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = 'assets'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodbTableName)

getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
healthPath = '/health'
assetPath = '/asset'
assetsPath = '/assets'

def lambda_handler(event, context)
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
    elif httpMethod == getMethod and path == assetPath:
        response = getAsset(event['queryStringParameters']['assetid'])
    elif httpMethod == getMethod and path == assetsPath:
        response = getassets()
    elif httpMethod == postMethod and path == assetPath:
        response = saveAsset(json.loads(event['body']))
    elif httpMethod == patchMethod and path == assetPath:
        requestBody = json.loads(event['body'])
        response = modifyAsset(requestBody['assetid'], requestBody['updateKey'], requestBody['updateValue'])
    elif httpMethod == deleteMethod and path == assetPath:
        requestBody = json.loads(event['body'])
        response = deleteMethod(requestBody['assetid'])
    else:
        response = buildResponse(404, 'Not Found')
        
    return response

def getasset(assetid):
    try:
        response = table.get_item(
            key= {
                'assetid' : assetid
            }
        )
        if 'Item' in response:
            return buildResponse(200, response['Item'])
        else:
            return buildResponse(404, {'Message' : 'AssetId: %s not found' % assetid})
    except:
        logger.exception('There is something wrong and handler is not working properly in getasset request')

def getAssets():
    try:
        response = table.scan()
        result = response['Item']
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Item'])
            
        body = {
            'assets' : response
        }
        return buildResponse(200, body)
    
    except:
        logger.exception('There is something wrong and handler is not working properly in getassets request')
        
def saveAsset(requestBody):
    try:
        table.put_item(Item=requestBody)
        body= {
            'Operation' : 'SAVE',
            'Message' :  'SUCCESS',
            'Item' : requestBody
        }
        return buildResponse(200, body)
    except:
        logger.exception('There is something wrong and handler is not working properly in saveAsset request')
        
def modifyAsset(assetid, updateKey, updateValue):
    try:
        response = table.update_item(
            key = {
                'assetid' : assetid
            },
            updateExpression='set %s = : value' % updateKey,
            ExpressionAttributeValue={
                ':value' : updateValue
            },
            returnValue='UPDATE_NEW'
        )
        body = {
            'Operation' : 'UPDATE',
            'Message' : 'SUCCESS',
            'UpdateAttribute' : response
        }
        return buildResponse(200, body)
    except:
        logger.exception('There is something wrong and handler is not working properly in modifyAsset request')
        
def deleteAsset(assetid):
    try:
        response = table.delete_item(
            key={
                'assetid' : assetid
            },
            ReturnValues='ALL_OLD'
        )
        body = {
            'Operation' : 'DELETE',
            'Message' : 'SUCCESS',
            'deleteItem' : response
        }
        return buildResponse(200, body)
    except:
        logger.exception('There is something wrong and handler is not working properly in deleteAsset request')
        
        
def buildResponse(statusCode, body=None):
    response = {
        'statusCode' : statusCode,
        'headers' : {
            'content-Type' : 'application/json',
            'Access-Control-Allow-Origin' : '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls=CustomeEncoder)
    return response