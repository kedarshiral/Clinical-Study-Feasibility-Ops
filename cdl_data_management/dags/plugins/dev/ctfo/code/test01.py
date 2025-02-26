import boto3
def list_folders(s3_client, bucket_name, prefix_value):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix= prefix_value, Delimiter='/')
    for content in response.get('CommonPrefixes', []):
        yield content.get('Prefix')

s3_client = boto3.client('s3')
bucket_name = ''
prefix_value = "clinical-data-lake/applications/commons/temp/kpi_output_dimension/f_rpt_country/"
folder_list = list_folders(s3_client, bucket_name, prefix_value)

data_dt_list = []

for folder in folder_list:
	
	data_dt_list.append(folder.split('pt_data_dt=')[1])
	#print(folder.split('pt_data_dt=')[1])
	
    #print('Folder found: %s' % folder)
	
	
print(max(data_dt_list)	)

prefix_value += "pt_data_dt=" + str(max(data_dt_list)) 

print(prefix_value)


folder_list = list_folders(s3_client, bucket_name, prefix_value)



cycle_id_list = []

for folder in folder_list:
	
	cycle_id_list.append(folder.split('pt_cycle_id=')[1])
	#print(folder.split('pt_cycle_id=')[1]
	
	
print(max(cycle_id_list)	)	

final_data_path = "s3:///" +  prefix_value + "pt_cycle_id=" + str(max(cycle_id_list))

print(final_data_path)