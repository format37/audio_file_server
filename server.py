import asyncio
from aiohttp import web
from audio_sql_proc import cdr_filenames
import os
from uuid import uuid4
import pandas as pd

WEBHOOK_PORT = 8082

async def call_filenames(request):
	
	answer = 'request received'
	
	data_path = '/home/alex/projects/audio_file_server/data/'
	data_uid = str(uuid4())
	file_path_data			= data_path+data_uid+'.csv'	

	# save data	
	with open(file_path_data, 'w') as source_file: 
		source_file.write(await request.text())
		source_file.close()
		#os.unlink(file_path_data) TODO: enable

		df = pd.read_csv(file_path_data,';',dtype = {'call_id': 'str', 'linkedid': 'str'})
		sql_result = cdr_filenames(df)

		answer = ''

		for row in sql_result:

			call_id		= row[0]
			linkedid	= row[1]
			filename	= row[2]

			last_slash_pos = filename.rindex('/')
			filename 	= filename[last_slash_pos+1:]

			answer += call_id+';'+linkedid+';'+filename+';'+'\n'
		
	return web.Response(text=answer,content_type="text/html")

app = web.Application(client_max_size=1024**3)
app.router.add_post('/filenames', call_filenames)

web.run_app(
    app,
    port=WEBHOOK_PORT,
)