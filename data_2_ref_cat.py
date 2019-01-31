###################################
#author: Scott D. McDermott
#date: 1/17/2019
#summary: Add reference values from Config file
###################################

import shutil, os
import json
import datetime

#Custom modules
import modules.shapefile_to_postgres as stp

#import config.config_dbase as db_config
#print(db_config.DATABASE_CONFIG['Postgres']['databases'][0]['host'])

dbasename = 'geo_cat_staging'
dserver = 'Postgres'
port = ''
hostname = '' 
username = '' 
pwd = '' 
shp_names = []
dbase_conn = {}
value_param = []

# Get the current date and time in UTC format
get_datetime = datetime.datetime.utcnow()

# Get the basic info for each data obtained	
json_config_feat = r'E:\platform\scripts\geo_cat\config\config_features.json'
json_config_data = r'E:\platform\scripts\geo_cat\config\config_dbase.json'
feat_json = ''
data_json = ''

# Read the Feature Config and build array for each stored proc
with open(json_config_feat) as f:
	feat_json = json.load(f)
	
# Get the geo_cat_data database info
with open(json_config_data) as f:
	data_json = json.load(f)
for j in data_json:
	if j == dserver:
		for j2 in data_json[j]['databases']:
			if j2['database_name'] == dbasename:
				len_dbases = len(data_json[j]['databases'])
				port = j2['port']
				hostname = j2['host']
				username = j2['user']
				pwd = j2['pwd']

dbase_conn = {
'host': hostname,
'dbname': dbasename,
'user': username,
'password': pwd,
'port': port
}			

# Build and array of required data from the features for the stored proce
# This avoids some looping hell
sp_values = []
for ft in feat_json:
	if ft != 'properties':
		source_id = ft
		for shp in feat_json[source_id]["shapefiles"]:
			feat_type = shp['geo_object_feature_type']
			feat_url = shp['source_url']
			shp_file_name = shp['file_feat_name']
			sp_values.append((ft, feat_type, feat_url, shp_file_name))
#print(sp_values)
			
# Get the stored procs and parameters from config file
# Open Postgres connection
sp_value_list = []
conn = ''

for j in data_json:
	if j == dserver:
		for j2 in data_json[j]["stored_proc"]:
			if j2['name'] == 'public.sp_insert_source_geo':
				s_per = ()
				i = 0
				s_per = list(s_per)
				sp_name =j2["name"]
				len_param = len(j2["param"])
				params = j2["param"]
				while i < len_param:
					s_per.insert(i, '%s')
					i += 1
				s_per = tuple(s_per)
				s_per =  "(" + ','.join(s_per) + ");"
				for d in sp_values:	
					conn = stp.p_conn(dbase_conn)				
					ft1 = d[0]
					ft2 = 1
					sp_value_list.append((ft1, ft2))
					#"source_description", "version_number"
					status_call = stp.call_sp_postgres(conn, sp_name, sp_value_list[0], s_per)
					sp_value_list = []
					status_call = ''
					ft1 = ''
					ft2 = ''				
					#closing database connection.  Need to close it to allow the next INSERT
					if(conn):
						conn.close()
			if j2['name'] == 'public.sp_insert_feature_type':
				s_per = ()
				i = 0
				s_per = list(s_per)
				sp_name =j2["name"]
				len_param = len(j2["param"])
				params = j2["param"]
				while i < len_param:
					s_per.insert(i, '%s')
					i += 1
				s_per = tuple(s_per)
				s_per =  "(" + ','.join(s_per) + ");"
				for d in sp_values:
					conn = stp.p_conn(dbase_conn)
					ft1 = d[3]
					sp_value_list.append((ft1))
					status_call = stp.call_sp_postgres(conn, sp_name, sp_value_list, s_per)
					sp_value_list = []
					status_call = ''
					ft1 = ''
					ft2 = ''				
					#closing database connection.  Need to close it to allow the next INSERT
					if(conn):
						conn.close()
			if j2['name'] == 'public.sp_insert_source_url':
				s_per = ()
				i = 0
				s_per = list(s_per)
				sp_name =j2["name"]
				len_param = len(j2["param"])
				params = j2["param"]
				while i < len_param:
					s_per.insert(i, ' %s')
					i += 1
				s_per = tuple(s_per)
				s_per =  "(" + ','.join(s_per) + ");"
				for d in sp_values:
					conn = stp.p_conn(dbase_conn)	
					fx_value_list = []
					ft1 = d[0]
					# get source id
					fx_value_list.append(d[0])
					source_id = stp.call_fx_postgres(conn, 'public.fx_get_id_source_geo', fx_value_list, '(%s);')
					for d in sp_values:
						ft2 = d[2]
						ft3 = 1
					sp_value_list.append((source_id, ft2, ft3))
					# source_id, source_url, version_number
					status_call = stp.call_sp_postgres(conn, sp_name, sp_value_list[0], s_per)
					sp_value_list = []
					status_call = ''
					ft1 = ''
					ft2 = ''					
					if(conn):
						conn.close()
# #closing database connection.
# if(conn):
	# conn.close()
