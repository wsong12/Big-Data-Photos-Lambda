import json
import urllib.parse
import boto3
import requests
print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    
    print("event: ", event)
    print("context: ", context)

    # Get the object from the event and show its content type
    bucket = 'photos.wanjias.us'
    key=event['Records'][0]['s3']['object']['key']

    labels=detect_labels(key, bucket)
    response=s3.head_object(Bucket=bucket, Key=key)
    try:
        tags=response['Metadata']['customlabels'].split(",")
        for i in range(len(tags)):
            tags[i] = tags[i].capitalize()
            if(tags[i] != "" and tags[i] not in labels):
                labels.append(tags[i])
    except Exception as e:
        print(e)
    
    
    print("labels:", labels)
    
    photo_dict = {
        "objectKey":key[7:],
        "bucket":bucket,
        "createdTimestamp": event['Records'][0]['eventTime'],
        "labels":labels
    }
    
    index_dict = json.dumps({"index":{"_index":"photos", "_id": key[7:]}})
    photo_json = json.dumps(photo_dict)
    
    request=index_dict+"\n"+ photo_json+"\n"
    
    headers = {'Content-Type': 'application/json'}
    
    r = requests.post("https://search-photos-dqvfdazm3c7h76iw5emb6nuo7u.us-east-1.es.amazonaws.com/_bulk", auth=('master','Helloworld1!'), headers=headers, data=request)
    print(r)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    

def detect_labels(photo, bucket):
    client=boto3.client('rekognition')

    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}},
        MaxLabels=10)

    print('Detected labels for ' + photo) 
    l=[] 
    for label in response['Labels']:
        l.append(label['Name'])
    return l
