import boto3
s3 = boto3.resource('s3')

# Bucket creation:
s3 = boto3.resource('s3')
s3.create_bucket(
	Bucket = 'it-attiliodivicino-datacont',
	CreateBucketConfiguration={
		'LocationConstraint': 'us-west-2'
	}
)