import asyncio
from aiohttp import web
from audio_sql_proc import cdr_filenames
import os
from uuid import uuid4

WEBHOOK_PORT = 8082

async def call_filenames(request):
	
	answer = 'request received'
	
	data_path = '/home/alex/projectaudio_file_server/data/'
	data_uid = str(uuid4())
	file_path_data			= data_path+data_uid+'.csv'	

	# save data	
	with open(file_path_data, 'w') as source_file: 
		source_file.write(await request.text())
		source_file.close()
		
	# read data
	#with open(file_path_data, 'rb') as source_file:
	#	lines = source_file.readlines()
	#	source_file.close()
		#os.unlink(file_path_data) TODO: enable

	df = pd.read_csv(file_path_data,';')
	
	linkedid = []
	linkedid.append(
		{
		'call_id':'0012388684',
		'date_from':'2020-10-15 22:15:06',
		'date_to':'2020-10-15 22:17:06',
		'linkedid':'1602789350.14400182',
		}
	)
	linkedid.append(
		{
		'call_id':'0012388685',
		'date_from':'2020-10-15 22:18:25',
		'date_to':'2020-10-15 22:20:25',
		'linkedid':'1602789550.14400185',
		}
	)
	sql_result = cdr_filenames(linkedid)

	answer = ''

	for row in sql_result:

		call_id		= row[0]
		linkedid	= row[1]
		filename	= row[2]

		last_slash_pos = filename.rindex('/')
		filename 	= filename[last_slash_pos+1:]

		answer += call_id+' - '+linkedid+' - '+filename+' - '+'<br>'
		
	return web.Response(text=answer,content_type="text/html")

app = web.Application(client_max_size=1024**3)
app.router.add_post('/filenames', call_filenames)

web.run_app(
    app,
    port=WEBHOOK_PORT,
)