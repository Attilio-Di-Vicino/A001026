# Upload data to the table:
import boto3, csv
dyndb=boto3.resource('dynamodb',region_name='us-west-2')

table = dyndb.Table("DataTable")
urlbase = "https://s3-us-west-2.amazonaws.com/it-attiliodivicino-datacont/"
with open( 's3://it-attiliodivicino-datacont/experiments.csv' , 'rb') as csvfile:
	csvf = csv.reader(csvfile,
		delimiter =',',
		quotechar='|'
	)
	for item in csvf:
		body=open('https://it-attiliodivicino-datacont.s3.us-west-2.amazonaws.com/\\'+item[3],'rb')
		s3.Object('datacont',item[3]).put(Body=body)
		md=s3.Object('datacont',item[3]).Acl().put(ACL='public-read')
		url= urlbase +item [3]
		metadata_item={
			'PartitionKey':item[0],
			'RowKey':item[1],
			'Description':item[4],
			'Date':item[2],
			'Url':url}
		table.put_item(Item= metadata_item)