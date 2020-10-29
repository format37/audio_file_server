import asyncio
from aiohttp import web
from audio_sql_proc import cdr_filenames

WEBHOOK_PORT = 8082
WEBHOOK_LISTEN = '0.0.0.0'  # In some VPS you may need to put here the IP addr

async def call_test(request):
	return web.Response(text='ok',content_type="text/html")

async def call_filenames(request):
	#question = request.rel_url.query['question']	
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
	answer = 'is: '
	for row in sql_result:
		call_id		= row[0]
		linkedid	= row[1]
		filename	= row[2]
		
		last_slash_pos = filename.rindex('/')
		filename 	= filename[last_slash_pos+1:]
		
		answer += call_id+' - '+linkedid+' - '+filename+' - '+'<br>'
	return web.Response(text=answer,content_type="text/html")

app = web.Application()
app.router.add_route('GET', '/test', call_test)
app.router.add_route('GET', '/filenames', call_filenames)

web.run_app(
    app,
    host=WEBHOOK_LISTEN,
    port=WEBHOOK_PORT,
)