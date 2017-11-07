# installation
```
pip install aiohttp
pip install aiobotocore
pip install aioamqp
pip install motor 
pip install termcolor 
```

## what dependencies are for ... 
 - aiohttp - our web server 
 - aiobotocore - communication with aws 
 - aioamqp - communication with rabbitmq (to come down the line)
 - motor - async mongodb interaction
    - https://motor.readthedocs.io/en/stable/tutorial-asyncio.html
 - term color for colored output based on worker number 
 - aiofiles - writing out files asynchronously
 - aiojson - writing json files asynchronously


# configuration 
```json
{
    "recheck_interval": 60, // minimum time in seconds between runs of a job, if a job appears more than once in this interval it will be delayed until the time is up
    "workers": 4, // as many as you have available threads ideally
}