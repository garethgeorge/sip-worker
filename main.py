from aiohttp import web 
import asyncio

from config import config 
from worker import worker_entrypoint

app = web.Application()


async def route_data(request):
    # TODO: write code here that gets the latest data for the provided 
    # region, az, etc...

    data = await request.json()

    resultingData = json.dumps({
        "test": "hello world!",
    })

    return web.Response(text=resultingData)

app.router.add_post('/data', route_data)

app.on_startup.append(lambda app: worker_entrypoint(app.loop))

web.run_app(app, port=3000)
