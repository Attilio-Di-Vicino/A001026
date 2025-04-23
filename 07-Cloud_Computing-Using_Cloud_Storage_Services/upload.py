# Upload a file:
import boto3

s3 = boto3.resource('s3')
s3.Object('datacont', 'test.jpg' ).put(
	Body=open( '/home/mydata/test.jpg' , 'rb')
)

# Using an already defined DynamoDB table:
dyndb=boto3.resource('dynamodb',region_name='us-west-2')
table = dyndb.Table("DataTable")