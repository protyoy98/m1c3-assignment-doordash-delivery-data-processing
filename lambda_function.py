import json
import io
import boto3 #will be available in aws lambda env
import pandas as pd #we need to configure

def read_s3_json_to_dataframe(s3_client_name,bucket_name, object_key):
    obj = s3_client_name.get_object(Bucket=bucket_name, Key=object_key)
    data = obj['Body'].read().decode('utf-8')
    # Split the multiline JSON data into individual JSON objects
    json_data_list = data.strip().split('\n')

    # Parse each JSON object and append to a list
    json_objects = []
    for json_str in json_data_list:
        json_objects.append(json.loads(json_str))

    # Convert the list of JSON objects to a Pandas DataFrame
    df = pd.DataFrame(json_objects)
    return df


def write_dataframe_to_s3(s3_client_name, dataframe, bucket_name, key):
    # Convert DataFrame to JSON string
    json_data = dataframe.to_json(orient='records', lines=True)

    # Create a BytesIO object to write the JSON string
    with io.BytesIO(str.encode(json_data)) as data_stream:
        # Upload the JSON file to S3
        s3_client_name.upload_fileobj(data_stream, bucket_name, key)


def publish_sns_message(sns_client_name, sns_arn, message_subject, message_body):
    sns_client_name.publish(Subject=message_subject,TargetArn=sns_arn, Message=message_body, MessageStructure='text')


def lambda_handler(event, context):
    # TODO implement
    source_bucket_name = event['Records'][0]['s3']['bucket']['name']
    source_object_key = event['Records'][0]['s3']['object']['key']
    target_bucket_name = 'doordash-target-zone-protyoy'
    target_object_key = 'delivered_doordash_orders.json'
    sns_arn_to_publish = 'arn:aws:sns:us-east-1:339712975909:doordash-lambda-run-notification'

    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')

    try:
        # read from json into dataframe
        df = read_s3_json_to_dataframe(s3_client, source_bucket_name, source_object_key)
        print("successfully read json to dataframe")

        #filter dataframe upon status = delivered
        df_delivered = df[df['status'] == "delivered"]
        print("filtered records based on given cindition")

        #write dataframe to json in s3
        write_dataframe_to_s3(s3_client, df_delivered, target_bucket_name, target_object_key)
        print(f"Dataframe saved as json in 's3://{target_bucket_name}' as '{target_object_key}' successfully")

        #publish sns message
        subject = 'DATA PROCESSING SUCCESSFUL'
        body = f"Successfully read json data from s3://{source_bucket_name}/{source_object_key}. Conditions have been applied. Saved as json file in s3://{target_bucket_name}/{target_object_key}"
        publish_sns_message(sns_client, sns_arn_to_publish, subject, body)
    
    except Exception as e:
        subject = 'DATA PROCESSING FAILURE'
        body = f"Error occured!!!\n {e}"
        publish_sns_message(sns_client, sns_arn_to_publish, subject, body)