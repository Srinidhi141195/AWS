import boto3
import json
import time

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    textract_client = boto3.client('textract')
    comprehend_client = boto3.client('comprehend')
    dynamodb_client = boto3.client('dynamodb')

    for record in event['Records']:
        try:
            if record.get('eventSource') == 'aws:s3':
                bucket = record['s3']['bucket']['name']
                key = record['s3']['object']['key']

                # Start Textract job
                response = textract_client.start_document_text_detection(
                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
                )
                job_id = response['JobId']

                # Polling Textract job status
                while True:
                    textract_response = textract_client.get_document_text_detection(JobId=job_id)
                    status = textract_response.get('JobStatus')
                    if status in ['SUCCEEDED', 'FAILED']:
                        break
                    time.sleep(5)

                if status == 'FAILED':
                    print(f"Textract processing failed for {key}")
                    continue  # Consider adding more robust error handling

                # Extract text from Textract response
                extracted_text = ' '.join([block['Text'] for block in textract_response['Blocks'] if block['BlockType'] == 'LINE'])

                # Perform sentiment analysis
                sentiment_response = comprehend_client.detect_sentiment(
                    Text=extracted_text[:5000],  # Comprehend supports up to 5000 bytes per request
                    LanguageCode='en'
                )

                contains_s3 = 's3' in extracted_text.lower()

                # Storing results in DynamoDB
                dynamodb_client.put_item(
                    TableName='pdf-metadata',  # Ensure this matches your DynamoDB table name
                    Item={
                        'id': {'S': key},  # Adjust the primary key field as per your table's schema
                        'Sentiment': {'S': sentiment_response['Sentiment']},
                        'SentimentScore': {'S': json.dumps(sentiment_response['SentimentScore'])},
                        'ContainsS3': {'BOOL': contains_s3}
                    }
                )

        except Exception as e:
            # Log the error
            print(f"Error processing record {record}: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps('PDF processing complete')
    }
