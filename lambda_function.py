import json
import logging
import boto3
import os

s3 = boto3.client('s3')
# dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
priority_inference = os.environ["priority_inference"]

logger = logging.getLogger("SEP_Priority_Checker")
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("The input event is %s",event)
    try:
        body = json.loads(event['body'])
        level = body['level']
        uuid = body['uuid']
        complaint = body['complaint']
        max_level = max(level.values())
        logger.info(max_level)
        max_key = next(key for key, value in level.items() if value == max_level)
        logger.info(max_key)
        payload = {"complaint":complaint, "level":int(max_key)}
        logger.info("Payload---------")
        logger.info(payload)
        payload_str = json.dumps(payload)
        payload_bytes = payload_str.encode('utf-8')
        ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
        runtime= boto3.client('runtime.sagemaker')
        response = runtime.invoke_endpoint(EndpointName = ENDPOINT_NAME,
                                       ContentType='application/json',
                                       Body=payload_bytes)
        logger.info(response)
        result = json.loads(response['Body'].read().decode())
        logger.info(result)
        final_result={
            'uuid':uuid,
            'priority': result["priority"],
            'reason': result["reason"]
        }
        try:
            response = create_table(priority_inference)
            print(response)
        except Exception as table_exception:
            logger.error("Error in %s creating the table",table_exception)
            exception_text = str(table_exception)
            logger.info(exception_text)
        try:
            response = insert_data(priority_inference ,uuid, result)
            logger.info("item has been created")
        except Exception as insert_exception:
            logger.error("Error in %s saving the raw data",insert_exception)
            exception_text = str(insert_exception)
            logger.info(exception_text)
            return {
                'statusCode': 503,
                'body': json.dumps(str(exception_text))
            }
        return {
            'statusCode': 200,
            'body': json.dumps(final_result)
        }
        
    except Exception as exc:
        logger.info(exc)
        logger.error(f"Exception i.e. {str(exc)} has occured.")
        prediction = {'priority':0 ,'reason':'error occured', 'uuid':body['uuid']}
        return prediction

def create_table(table_name):
    # Create DynamoDB table
    try:
        create_table_response = dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'uuid',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'uuid',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        logger.info(f"Table {table_name} created successfully.")
        return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB table created')
        }
    except dynamodb_client.exceptions.ResourceInUseException:
        logger.info(f"Table {table_name} already exists.")
        
def insert_data(table_name, item_uuid, result):
    # Put item into table
    try:
        put_item_response = dynamodb_client.put_item(
            TableName= table_name,
            Item={
                'uuid': {'S': str(item_uuid)},
                'results': {'S': str(result)}
            })
        logger.info(f"Item with uuid {item_uuid} added successfully.")
    except Exception as e:
        logger.info(f"Error putting item: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB item setup completed.')
    }
